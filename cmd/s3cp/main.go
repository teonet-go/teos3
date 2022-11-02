// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The s3cp copy file to/from S3 storage
package main

import (
	"flag"
	"fmt"
	"os"
)

const (
	appName    = "s3cp"
	appVersion = "0.0.1"
)

var (
	accessKey = os.Getenv("TEOS3_ACCESSKEY")
	secretKey = os.Getenv("TEOS3_SECRETKEY")
	endpoint  = os.Getenv("TEOS3_ENDPOINT")
	bucket    = os.Getenv("TEOS3_BUCKET")
	secure    = true
)

const usage = "s3cp [flags] source target"

func main() {
	fmt.Println("Teonet " + appName + " application ver " + appVersion)

	// Application parameters
	flag.StringVar(&accessKey, "accesskey", accessKey, "S3 storage Access key")
	flag.StringVar(&secretKey, "secretkey", secretKey, "S3 storage Secret key")
	flag.StringVar(&endpoint, "endpoint", endpoint, "S3 storage Endpoint")
	flag.StringVar(&bucket, "bucket", bucket, "S3 storage Bucket")
	flag.BoolVar(&secure, "secure", secure, "set secure=false to enable insecure (HTTP) access")

	args := flag.Args()
	if len(args) < 2 {
		fmt.Println(usage)
		fmt.Println(args)
		flag.Usage()
		os.Exit(0)
	}

}
