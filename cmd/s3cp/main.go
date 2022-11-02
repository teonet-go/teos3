// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The s3cp copy file to/from S3 storage
package main

import (
	"flag"
	"fmt"
	"log"
	"log/syslog"
	"os"
	"strings"

	"github.com/teonet-go/teos3"
)

// Application constants
const (
	appName    = "s3cp"
	appVersion = "0.0.1"
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
	var source []byte
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

		switch i {

		// Source
		case 0:
			if s3 {
				// get S3 object
				source, err = connectS3().Map.Get(key)
			} else {
				// get file
				source, err = os.ReadFile(key)
			}
			if err != nil {
				logError(err)
			}
			log.Println("got data from", args[i])

		// Target
		case 1:
			if s3 {
				// save S3 object
				err = connectS3().Map.Set(key, source)
			} else {
				// save file
				err = os.WriteFile(key, source, 0644)
			}
			if err != nil {
				logError(err)
			}
			log.Println("set data to", args[i])
		}
	}
}
