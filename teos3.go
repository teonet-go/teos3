// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// teos3 package contains Golang functions to rasy use AWS S3 storage as
// KeyValue Database. To use it create next buckets in your S3 storage:
//
//	teos3/map
package teos3

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const Version = "0.0.4"

const teoS3bucket = "teos3"

// TeoS3 methods receiver
type TeoS3 struct {
	con *minio.Client
	*Map
}

// Connect to teonet S3 storage
func Connect(accessKey, secretKey, endpoint string, secure bool, buckets ...string) (teos3 *TeoS3, err error) {
	teos3 = new(TeoS3)
	teos3.con, err = minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return
	}

	var bucket = teoS3bucket
	if len(buckets) > 0 && len(buckets[0]) > 0 {
		bucket = buckets[0]
	}
	teos3.Map = &Map{teos3.con, bucket}
	return
}

// Map methods receiver
type Map struct {
	con    *minio.Client
	bucket string
}

// Map data struct
type MapData struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// Set data to map by key
func (m *Map) Set(key string, data []byte) (err error) {
	reader := bytes.NewReader(data)
	err = m.SetObject(key, reader, int64(len(data)))
	return
}

// Set object to map by key
func (m *Map) SetObject(key string, reader io.Reader, objectSize int64) (err error) {
	_, err = m.con.PutObject(context.Background(), m.bucket, key, reader, objectSize,
		minio.PutObjectOptions{},
	)
	return
}

// Get map data by key
func (m *Map) Get(key string) (data []byte, err error) {

	obj, err := m.GetObject(key)
	if err != nil {
		return
	}
	defer obj.Close()

	// Read from raw object
	buf := new(bytes.Buffer)
	buf.ReadFrom(obj)
	data = buf.Bytes()

	return
}

// Get map object by key
func (m *Map) GetObject(key string) (obj *minio.Object, err error) {
	obj, err = m.con.GetObject(context.Background(), m.bucket, key,
		minio.GetObjectOptions{},
	)
	return
}

// Del remove key frim map by key
func (m *Map) Del(key string) (err error) {
	err = m.con.RemoveObject(context.Background(), m.bucket, key,
		minio.RemoveObjectOptions{},
	)
	return
}

// Get list of map keys by prefix
func (m *Map) List(prefix string) (keys chan string) {

	keys = make(chan string, 1)

	go func() {
		objInfo := m.con.ListObjects(context.Background(), m.bucket,
			minio.ListObjectsOptions{
				Prefix: prefix,
			},
		)
		for obj := range objInfo {
			keys <- obj.Key
		}
		close(keys)
	}()

	return
}

// Get string array of map keys by prefix
func (m *Map) ListAr(prefix string) (list []string) {

	objInfo := m.con.ListObjects(context.Background(), m.bucket,
		minio.ListObjectsOptions{
			Prefix: prefix,
		},
	)
	for obj := range objInfo {
		list = append(list, obj.Key)
	}

	return
}

// ListBody get all keys values by prefix asynchronously
func (m *Map) ListBody(prefix string) (mapDatas chan MapData) {

	mapDatas = make(chan MapData, 1)

	objInfo := m.con.ListObjects(context.Background(), m.bucket,
		minio.ListObjectsOptions{
			Prefix: prefix,
		},
	)

	var wg sync.WaitGroup
	for obj := range objInfo {
		wg.Add(1)
		go func(obj minio.ObjectInfo) {
			defer wg.Done()
			data, err := m.Get(obj.Key)
			if err != nil {
				return
			}
			mapDatas <- MapData{obj.Key, data}
		}(obj)

	}
	go func() {
		wg.Wait()
		close(mapDatas)
	}()

	return
}

// Copy from source to target. Source or Target may be s3 storage object. Use
// 's3:' prefix to define s3 object
func Copy(accessKey, secretKey, endpoint string, args []string, secures ...bool) (err error) {

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
		con, err = Connect(accessKey, secretKey, endpoint, secure)
		if err != nil {
			log.Fatalln(err)
		}
		log.Println("connect to s3 storage")
		connected = true
		teoS3conn = con
		return
	}

	var sourceObj io.Reader
	var sourceLen int64
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
				obj, err := connectS3().Map.GetObject(key)
				if err != nil {
					logError(err)
					return err
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
				err = connectS3().Map.SetObject(key, sourceObj, sourceLen)
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
