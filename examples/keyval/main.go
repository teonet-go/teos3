// Copyright 2022-23 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// This application get s3 object and save it to local file. This sample used
// minio-go package which help us create the teos3 package
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
	appName    = "Key value S3 Storage example"
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

	// Set and Get records as Key Value
	var wg sync.WaitGroup
	const num = 10
	const prefix = "test/key-"
	log.Println("Set and Get records:")
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

			// Get key from TeoS3 Map
			data, err := con.Get(key)
			if err != nil {
				log.Fatalln(err)
			}
			log.Println("Got data:", string(data))
			wg.Done()
		}(i)
	}
	wg.Wait()

	// Get list of keys and print it
	log.Println("Get list of keys:")
	keys := con.List(prefix)

	// Get keys from list asynchronously
	log.Println("Get keys from list asynchronously:")
	var ch = make(chan teos3.MapData, num)
	for key := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			data, err := con.Get(key)
			if err != nil {
				log.Fatalln(err)
			}
			ch <- teos3.MapData{Key: key, Value: data}
		}(key)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	for m := range ch {
		log.Println("Got data:", m.Key, string(m.Value))
	}

	// Get list of keys and values
	log.Println("Get list of keys and values:")
	mapData := con.ListBody(prefix)
	for m := range mapData {
		log.Println(m.Key, string(m.Value))
	}

	// Temporaly skip remove keys
	// return

	// Remove keys by key asynchronously
	log.Println("Remove keys by keys in list:")
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
