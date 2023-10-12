// Copyright 2023 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// TeoS3 `keyval-opt` example. There is example which shows how to used teos3
// packets functions with options.
//
// This example executes following tasks:
//
// - sets some numer of data records (objects) by key (objects name) to the
// key/value db baset on s3 bucket;
//
// - gets full list of saved data records by prefix;
//
// - gets part of list using options;
//
// - deletes all saved objects.
//
// Fill next environment variables to run this example:
//
//	export TEOS3_ACCESSKEY=YOUR_ACCESSKEY TEOS3_SECRETKEY=YOUR_SECRETKEY \
//	TEOS3_ENDPOINT=YOUR_GATEWAT TEOS3_BUCKET=YOUR_BUCKET
//
// Use next command to run this example:
//
//	go run ./examples/keyval-opts/
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/teonet-go/teos3"
)

const (
	appName    = "Key value S3 Storage acess example with options"
	appVersion = teos3.Version
)

var (
	accessKey = os.Getenv("TEOS3_ACCESSKEY")
	secretKey = os.Getenv("TEOS3_SECRETKEY")
	endpoint  = os.Getenv("TEOS3_ENDPOINT")
	bucket    = os.Getenv("TEOS3_BUCKET")
	secure    = true
)

func main() {

	// Application logo
	fmt.Println(appName + " ver " + appVersion)

	// Application parameters
	flag.StringVar(&accessKey, "accesskey", accessKey, "S3 storage Access key")
	flag.StringVar(&secretKey, "secretkey", secretKey, "S3 storage Secret key")
	flag.StringVar(&endpoint, "endpoint", endpoint, "S3 storage Endpoint")
	flag.StringVar(&bucket, "bucket", bucket, "S3 storage Bucket")
	flag.BoolVar(&secure, "secure", secure, "set secure=false to enable insecure (HTTP) access")

	flag.Parse()
	if len(accessKey) == 0 || len(secretKey) == 0 || len(endpoint) == 0 {
		fmt.Println("The accesskey, secretkey and endpoint is requered parameters.")
		flag.Usage()
		return
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	start := time.Now()

	// Connect to teonet S3 storage
	log.Println("Connect to", endpoint)
	con, err := teos3.Connect(accessKey, secretKey, endpoint, secure, bucket)
	if err != nil {
		log.Fatalln(err)
	}

	// Set some key/value records to the S3 storage asynchronously
	const num = 10
	var wg sync.WaitGroup
	const prefix = "test/key-"
	log.Println("Set records:")
	for i := 1; i <= num; i++ {
		// Key values to set and get
		wg.Add(1)
		go func(i int) {
			key := fmt.Sprintf(prefix+"%02d", i)
			data := []byte(fmt.Sprintf("Hello %02d from TeoS3 Map!", i))

			// Set key to TeoS3 Map
			err = con.Set(key, data)
			if err != nil {
				log.Fatalln(err)
			}

			log.Println("Set data:", string(data))
			wg.Done()
		}(i)
	}
	wg.Wait()

	// Set list options and Get part of list using ListOptions
	opt := con.NewListOptions().SetMaxKeys(3).SetStartAfter("test/key-03")
	log.Println("Get part of list using ListObjectsOptions:")
	fmt.Printf(
		"Using list options on list len %d, start after: %s, num keys: %d\n",
		con.ListLen(prefix), opt.StartAfter, opt.MaxKeys,
	)
	keys := con.List(prefix, opt)
	for key := range keys {
		fmt.Println("  key", key)
	}

	// Set record with options
	// setopt := *new(teos3.SetOptions).SetRetainUntilDate(time.Now().UTC().Add(time.Hour * 48))
	// const extraKey = "extra-key"
	// err = con.Set(extraKey, []byte("some data"), setopt)
	// fmt.Println("set extra key:", err)
	// //time.Sleep(2 * time.Second)
	// data, err := con.Get(extraKey)
	// fmt.Println("get extra key:", data, err)
	// con.Del(extraKey)

	// Remove keys by prefix asynchronously
	log.Println("Get list by prefix and remove all keys by lists keys:")
	keys = con.List(prefix)
	for key := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			err := con.Del(key)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Println("  del", key)
		}(key)
	}
	wg.Wait()

	log.Println("All done", time.Since(start))
}
