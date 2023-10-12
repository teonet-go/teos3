// Copyright 2022-23 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The TeoS3 package, Copy module.

package teos3

import (
	"bufio"
	"io"
	"log"
	"os"
	"strings"
)

// Copy copys s3 object from source to target. Source or Target may be s3
// storage object. Use 's3:' prefix to define s3 object.
func Copy(accessKey, secretKey, endpoint, bucket string, args []string,
	secures ...bool) (err error) {

	// The secure defaul true
	var secure = true
	if len(secures) > 0 {
		secure = secures[0]
	}

	// Connect to teonet S3 storage
	var connected bool
	var teoS3conn *TeoS3
	connectS3 := func() (con *TeoS3) {
		if connected {
			con = teoS3conn
			return con
		}
		con, err = Connect(accessKey, secretKey, endpoint, secure, bucket)
		if err != nil {
			log.Fatalln(err)
		}
		log.Println("connect to s3 storage")
		connected = true
		teoS3conn = con
		return
	}

	var sourceLen int64
	var sourceObj io.Reader
	for i := range args {

		// Trim and check S3 prefix in arguments
		args[i] = strings.Trim(args[i], " \t")
		s3 := strings.Index(args[i], "s3:") == 0
		var key string
		if s3 {
			key = args[i][3:]
		} else {
			key = args[i]
		}

		// Log error, get and set functions
		logError := func(err error) {
			log.Println("error", err)
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
				obj, err := connectS3().GetObject(key)
				if err != nil {
					logError(err)
					return err
				}
				defer obj.Close()
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
				return err
			}
			defer file.Close()
			sourceObj = bufio.NewReader(file)
			fileStat, _ := file.Stat()
			sourceLen = fileStat.Size()
			logSet(args[i])

		case Target:

			// Save source to S3
			if s3 {
				err = connectS3().SetObject(key, sourceObj, sourceLen)
				if err != nil {
					logError(err)
					return
				}
				logGet(args[i])
				continue
			}

			// Save source to file
			fo, err := os.Create(key)
			if err != nil {
				logError(err)
				return err
			}
			bufio.NewWriter(fo).ReadFrom(sourceObj)
			fo.Close()
			logGet(args[i])
		}
	}

	return
}
