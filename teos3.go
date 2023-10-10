// Copyright 2022-23 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The TeoS3 package contains Golang functions that make it easy to use S3
// storage as a key-value database.
// This package uses [minio-go](https://github.com/minio/minio-go) S3 client.
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

const Version = "0.1.0"

const teoS3bucket = "teos3"

// TeoS3 methods receiver
type TeoS3 struct {
	con    *minio.Client
	bucket string
}

// TeoS3Options is TeoS3 commands options
type TeoS3Options struct {
	context.Context
}

// Connect creates new cinnwction to S3 storage using accessKey, secretKey,
// endpoint, secure flag and bucket (if ommited then default 'teos3' buckets
// name used). The enpoind argument must be specified without http/https
// prefix(just domain and path), and the secure argument defines HTTPS if true
// or HTTP if false.
func Connect(accessKey, secretKey, endpoint string, secure bool,
	buckets ...string) (teos3 *TeoS3, err error) {

	teos3 = new(TeoS3)
	if teos3.con, err = minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	}); err != nil {
		return
	}

	if len(buckets) > 0 && len(buckets[0]) > 0 {
		teos3.bucket = buckets[0]
		return
	}
	teos3.bucket = teoS3bucket
	return
}

// SetObjectOptions contains context.Context and options specified by user for
// Set requests
type SetObjectOptions struct {
	context.Context
	minio.PutObjectOptions
}

// Set sets data to map by key. The options parameter may be ommited and
// than default SetObjectOptions with context.Background and empty
// minio.PutObjectOptions used.
func (m *TeoS3) Set(key string, data []byte, options ...SetObjectOptions) error {
	return m.SetObject(key, bytes.NewReader(data), int64(len(data)), options...)
}

// SetObject sets object to map by key. The options parameter may be ommited and
// than default SetObjectOptions with context.Background and empty
// minio.PutObjectOptions used.
func (m *TeoS3) SetObject(key string, reader io.Reader, objectSize int64,
	options ...SetObjectOptions) (err error) {

	// Set options
	var opt *SetObjectOptions
	if len(options) > 0 {
		opt = &options[0]
	} else {
		opt = &SetObjectOptions{
			Context:          context.Background(),
			PutObjectOptions: minio.PutObjectOptions{},
		}
	}

	_, err = m.con.PutObject(opt.Context, m.bucket, key, reader, objectSize,
		opt.PutObjectOptions,
	)
	return
}

// GetObjectOptions contains context.Context and options are used to specify
// additional headers or options during GET requests.
type GetObjectOptions struct {
	context.Context
	minio.GetObjectOptions
}

// Get map data by key. The options parameter may be ommited and than default
// GetObjectOptions with context.Background and empty minio.SetObjectOptions
// used.
func (m *TeoS3) Get(key string, options ...GetObjectOptions) (
	data []byte, err error) {

	obj, err := m.GetObject(key, options...)
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

// Get map object by key. The options parameter may be ommited and than default
// GetObjectOptions with context.Background and empty minio.SetObjectOptions
// used.
func (m *TeoS3) GetObject(key string, options ...GetObjectOptions) (
	*minio.Object, error) {

	// Set options
	var opt *GetObjectOptions
	if len(options) > 0 {
		opt = &options[0]
	} else {
		opt = &GetObjectOptions{
			Context:          context.Background(),
			GetObjectOptions: minio.GetObjectOptions{},
		}
	}

	return m.con.GetObject(opt.Context, m.bucket, key, opt.GetObjectOptions)
}

// DelObjectOptions contains context.Context and options for Remove
// requests.
type DelObjectOptions struct {
	context.Context
	minio.RemoveObjectOptions
}

// Del remove key from map by key. The options parameter may be ommited and than
// default DelObjectOptions with context.Background and empty
// minio.RemoveObjectOptions used.
func (m *TeoS3) Del(key string, options ...DelObjectOptions) (err error) {

	// Set options
	var opt *DelObjectOptions
	if len(options) > 0 {
		opt = &options[0]
	} else {
		opt = &DelObjectOptions{
			Context:             context.Background(),
			RemoveObjectOptions: minio.RemoveObjectOptions{},
		}
	}

	return m.con.RemoveObject(opt.Context, m.bucket, key, opt.RemoveObjectOptions)
}

// ListObjectsOptions contains context.Context and options for List requests.
type ListObjectsOptions struct {
	context.Context
	minio.ListObjectsOptions
}

// List gets list of map keys by prefix. The options parameter may be ommited
// and than default ListObjectsOptions with context.Background and empty
// minio.ListObjectsOptions used. The Prefix parameter of the ListObjectsOptions
// will be always overwritten with the prefix functions argument (so it may be
// empty).
func (m *TeoS3) List(prefix string, options ...ListObjectsOptions) (keys chan string) {

	// Get options from prefix and input options arguments
	opt := m.getListOptions(prefix, options...)

	// Get keys
	keys = make(chan string, 1)
	go func() {
		objInfo := m.con.ListObjects(opt.Context, m.bucket, opt.ListObjectsOptions)
		for obj := range objInfo {
			keys <- obj.Key
		}
		close(keys)
	}()

	return
}

// ListAr gets string array of map keys by prefix.
func (m *TeoS3) ListAr(prefix string, options ...ListObjectsOptions) (
	list []string) {

	// Get options from prefix and input options arguments
	opt := m.getListOptions(prefix, options...)

	objInfo := m.con.ListObjects(opt.Context, m.bucket, opt.ListObjectsOptions)
	for obj := range objInfo {
		list = append(list, obj.Key)
	}

	return
}

// Map data struct
type MapData struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// ListBody gets all keys and values in MapData struct by prefix asynchronously.
func (m *TeoS3) ListBody(prefix string) (mapDatas chan MapData) {

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

// getListOptions returns ListObjectsOptions created from input prefix and
// options arguments.
func (m *TeoS3) getListOptions(prefix string, options ...ListObjectsOptions) (
	opt *ListObjectsOptions) {

	if len(options) > 0 {
		opt = &options[0]
	} else {
		opt = &ListObjectsOptions{
			Context:            context.Background(),
			ListObjectsOptions: minio.ListObjectsOptions{},
		}
	}
	opt.Prefix = prefix
	return
}

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
