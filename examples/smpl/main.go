// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// This application used teoS3 package which contains Go functions to rasy use 
// S3 storage as Key Value Database. Use s3cp utilite to save teotun.jpeg which 
// used in this example: 
//   go run ./cmd/s3cp/ --bucket=tst ./examples/smpl/teotun.jpeg s3:/teotun.jpeg
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/teonet-go/teos3"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	appName    = "Simple S3 Storage example"
	appVersion = teos3.Version
)

const (
	bucket = "tst"
	object = "teotun.jpeg"
)

var (
	accessKey = os.Getenv("TEOS3_ACCESSKEY")
	secretKey = os.Getenv("TEOS3_SECRETKEY")
	endpoint  = os.Getenv("TEOS3_ENDPOINT")
	secure    = true
)

func main() {

	// Application logo
	fmt.Println(appName + " ver " + appVersion)

	// Application parameters
	flag.StringVar(&accessKey, "accesskey", accessKey, "S3 storage Access key")
	flag.StringVar(&secretKey, "secretkey", secretKey, "S3 storage Secret key")
	flag.StringVar(&endpoint, "endpoint", endpoint, "S3 storage Endpoint")
	flag.BoolVar(&secure, "secure", secure, "set secure=false to enable insecure (HTTP) access")

	flag.Parse()
	if len(accessKey) == 0 || len(secretKey) == 0 || len(endpoint) == 0 {
		fmt.Println("The accesskey, secretkey and endpoint is requered parameters.")
		flag.Usage()
		return
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	start := time.Now()

	// Requests are always secure (HTTPS) by default. Set secure=false to enable
	// insecure (HTTP) access. This boolean value is the last argument for New().
	//
	// New returns an Amazon S3 compatible client object. API compatibility
	// (v2 or v4) is automatically determined based on the Endpoint value.
	log.Println("Connecting to", endpoint, "secure", secure)
	s3Client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		log.Fatalln(err)
	}

	// Check bucket exists
	log.Println("Check bucket", bucket, "exists")
	found, err := s3Client.BucketExists(context.Background(), bucket)
	if err != nil {
		log.Fatalln(err)
	}
	if !found {
		log.Println("Create bucket", bucket)
		err = s3Client.MakeBucket(context.Background(), bucket,
			minio.MakeBucketOptions{},
		)
		if err != nil {
			log.Fatalln(err)
		}
	}

	// Get object
	log.Println("Get S3 bucket:", bucket, "object:", object)
	reader, err := s3Client.GetObject(context.Background(), bucket, object,
		minio.GetObjectOptions{},
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer reader.Close()

	// Create local file
	log.Println("Create local file", object)
	localFile, err := os.Create(object)
	if err != nil {
		log.Fatalln(err)
	}
	defer localFile.Close()

	// Get s3 object statistic
	log.Println("Get s3 statistics")
	stat, err := reader.Stat()
	if err != nil {
		log.Fatalln(err)
	}

	// Copy s3 object to local file
	log.Println("Copy s3 object to local file")
	if _, err := io.CopyN(localFile, reader, stat.Size); err != nil {
		log.Fatalln(err)
	}

	// Put file to S3
	log.Println("Put file to S3")
	s3Client.FPutObject(context.Background(), bucket, object+"(2)", object,
		minio.PutObjectOptions{
			ContentType: stat.ContentType,
		},
	)

	// List all objects from a bucket-name with a matching prefix.
	log.Println("List all objects from", bucket)
	for object := range s3Client.ListObjects(context.Background(), bucket,
		minio.ListObjectsOptions{
			UseV1: true,
			// Prefix:    "my-prefixname",
			Recursive: true,
		}) {
		if object.Err != nil {
			fmt.Println(" ", object.Err)
			return
		}
		fmt.Println(" ", object.Key)
	}

	log.Println("All done", time.Since(start))
}
