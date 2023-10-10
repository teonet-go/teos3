// Copyright 2022-23 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The s3cp application copy file to/from S3 storage.
//
// This application send logs to syslog. To read current log messages in
// archlinux use `journalctl -f` command.
//
// The S3 storage credentials may be set in application parameters or in
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
	"flag"
	"fmt"
	"log"
	"log/syslog"
	"os"

	"github.com/teonet-go/teos3"
)

// Application constants
const (
	appName    = "s3cp"
	appVersion = teos3.Version
)

// S3 access variables
var (
	accessKey = os.Getenv("TEOS3_ACCESSKEY")
	secretKey = os.Getenv("TEOS3_SECRETKEY")
	endpoint  = os.Getenv("TEOS3_ENDPOINT")
	bucket    = os.Getenv("TEOS3_BUCKET")
	secure    = true
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

	teos3.Copy(accessKey, secretKey, endpoint, bucket, args, secure)
}
