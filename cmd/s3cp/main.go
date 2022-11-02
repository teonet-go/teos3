// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The s3cp application copy file to/from S3 storage.
//
// This application send logs to syslog. To read curent log messages in
// archlinux use `journalctl -f` command.
//
// The S3 storage credintals may be set in application parameters or in
// environment variables:
//
//	TEOS3_ACCESSKEY
//	TEOS3_SECRETKEY
//	TEOS3_ENDPOINT
//	TEOS3_BUCKET
//
// Parameter and arguments usage:
// s3cp [OPTION] source target
// use s3:/folder_and_object_name to define S3 in source or target
//
// Usage of /tmp/go-build1982013444/b001/exe/s3cp:
//
//	-accesskey string
//	   S3 storage Access key
//	-bucket string
//	   S3 storage Bucket
//	-endpoint string
//	   S3 storage Endpoint
//	-secretkey string
//	   S3 storage Secret key
//	-secure
//	   set secure=false to enable insecure (HTTP) access (default true)
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"log/syslog"
	"os"
	"strings"

	"github.com/teonet-go/teos3"
)

// Application constants
const (
	appName    = "s3cp"
	appVersion = teos3.Version
)

// S3 access variables
var (
	accessKey, secretKey, endpoint, bucket string
	secure                                 = true
)

// Application usage message
const (
	about = "Teonet " + appName + " application ver " + appVersion + "\n"
	usage = "s3cp [OPTION] source target\n" +
		"use s3:/folder_and_object_name to define S3 in source or target\n"
)

func main() {

	// Set log otput to syslog
	sysLog, err := syslog.New(syslog.LOG_INFO|syslog.LOG_LOCAL7, appName)
	if err != nil {
		log.Fatalln(err)
	}
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	log.SetOutput(sysLog)

	// Application parameters
	flag.StringVar(&accessKey, "accesskey", accessKey, "S3 storage Access key")
	flag.StringVar(&secretKey, "secretkey", secretKey, "S3 storage Secret key")
	flag.StringVar(&endpoint, "endpoint", endpoint, "S3 storage Endpoint")
	flag.StringVar(&bucket, "bucket", bucket, "S3 storage Bucket")
	flag.BoolVar(&secure, "secure", secure, "set secure=false to enable insecure (HTTP) access")

	// Define new flag usage function and parse flag
	flagUsage := flag.Usage
	flag.Usage = func() {
		fmt.Print(about + "\n" + usage + "\n")
		flagUsage()
		fmt.Println()
	}
	flag.Parse()

	// Set parameters from env
	if len(accessKey) == 0 {
		accessKey = os.Getenv("TEOS3_ACCESSKEY")
	}
	if len(secretKey) == 0 {
		secretKey = os.Getenv("TEOS3_SECRETKEY")
	}
	if len(endpoint) == 0 {
		endpoint = os.Getenv("TEOS3_ENDPOINT")
	}
	if len(bucket) == 0 {
		bucket = os.Getenv("TEOS3_BUCKET")
	}

	// Check parameters
	if len(accessKey) == 0 || len(secretKey) == 0 || len(endpoint) == 0 {
		fmt.Print("Parameters -accesskey, -secretkey and -endpoint should be set\n")
		flag.Usage()
		os.Exit(0)
	}

	// Check arguments
	args := flag.Args()
	if len(args) < 2 {
		flag.Usage()
		os.Exit(0)
	}
	log.Println("copy", args[0], "to", args[1])

	// Connect to teonet S3 storage
	var connected bool
	var teoS3conn *teos3.TeoS3
	connectS3 := func() (con *teos3.TeoS3) {
		if connected {
			con = teoS3conn
			return con
		}
		con, err = teos3.Connect(accessKey, secretKey, endpoint, secure)
		if err != nil {
			log.Fatalln(err)
		}
		log.Println("connect to s3 storage")
		connected = true
		teoS3conn = con
		return
	}

	// Check parameters and copy file
	var sourceObj io.Reader
	var sourceLen int64
	for i := range args {

		// Trim and check S3 in argument
		args[i] = strings.Trim(args[i], " \t")
		s3 := strings.Index(args[i], "s3:") == 0
		var key string
		if s3 {
			key = args[i][3:]
		} else {
			key = args[i]
		}

		// Log error function
		logError := func(err error) {
			log.Fatalln("error", err)
		}
		logSet := func(key string) {
			log.Println("got data from", key)
		}
		logGet := func(key string) {
			log.Println("set data to", key)
		}

		// Argument type
		const (
			Source = iota
			Target
		)

		// Switch by argument type
		switch i {

		case Source:

			// Get S3 object
			if s3 {
				obj, err := connectS3().Map.GetObject(key)
				if err != nil {
					logError(err)
				}
				sourceObj = obj
				objStat, _ := obj.Stat()
				sourceLen = objStat.Size
				logSet(args[i])
				continue
			}

			// Get file
			file, err := os.Open(key)
			if err != nil {
				logError(err)
			}
			defer file.Close()
			sourceObj = bufio.NewReader(file)
			fileStat, _ := file.Stat()
			sourceLen = fileStat.Size()
			logSet(args[i])

		case Target:

			// Save source to S3
			if s3 {
				err = connectS3().Map.SetObject(key, sourceObj, sourceLen)
				if err != nil {
					logError(err)
				}
				logGet(args[i])
				continue
			}

			// Save source to file
			fo, err := os.Create(key)
			if err != nil {
				logError(err)
			}
			bufio.NewWriter(fo).ReadFrom(sourceObj)
			fo.Close()
			logGet(args[i])
		}
	}
}
