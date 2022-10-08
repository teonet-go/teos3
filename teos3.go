// Copyright 2022 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// teos3 package contains Golang functions to rasy use AWS S3 storage as
// KeyValue Database. To use it create next buckets in your S3 storage:
//
//	teos3/map
package teos3

import (
	"bytes"
	"context"
	"log"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const Version = "0.0.1"

const teoS3bucket = "teos3"

// TeoS3 methods receiver
type TeoS3 struct {
	con *minio.Client
	*Map
}

// Connect to teonet S3 storage
func Connect(accessKey, secretKey, endpoint string, secure bool) (teos3 *TeoS3, err error) {
	teos3 = new(TeoS3)
	teos3.con, err = minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		log.Fatalln(err)
	}
	teos3.Map = &Map{teos3.con, teoS3bucket}
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

	_, err = m.con.PutObject(context.Background(), m.bucket, key, reader, int64(len(data)),
		minio.PutObjectOptions{},
	)
	if err != nil {
		log.Fatalln(err)
	}
	// defer obj.Close()

	return
}

// Get map data by key
func (m *Map) Get(key string) (data []byte, err error) {

	obj, err := m.con.GetObject(context.Background(), m.bucket, key,
		minio.GetObjectOptions{},
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer obj.Close()

	// Read from raw object
	buf := new(bytes.Buffer)
	buf.ReadFrom(obj)
	data = buf.Bytes()

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
func (m *Map) List(prefix string) (result chan string, err error) {

	result = make(chan string, 1)

	go func() {
		objInfo := m.con.ListObjects(context.Background(), m.bucket,
			minio.ListObjectsOptions{
				Prefix: prefix,
			},
		)
		for obj := range objInfo {
			result <- obj.Key
		}
		close(result)
	}()

	return
}

// Get string array of map keys by prefix
func (m *Map) ListAr(prefix string) (list []string, err error) {

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
func (m *Map) ListBody(prefix string) (result chan MapData, err error) {

	result = make(chan MapData, 1)

	objInfo := m.con.ListObjects(context.Background(), m.bucket,
		minio.ListObjectsOptions{
			Prefix: prefix,
		},
	)

	var wg sync.WaitGroup
	for obj := range objInfo {

		wg.Add(1)
		go func(obj minio.ObjectInfo) {
			data, err := m.Get(obj.Key)
			if err != nil {
				return
			}
			result <- MapData{obj.Key, data}
			wg.Done()
		}(obj)

	}
	go func() {
		wg.Wait()
		close(result)
	}()

	return
}
