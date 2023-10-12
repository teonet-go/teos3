// Copyright 2022-23 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The TeoS3 package contains Golang functions that make it easy to use S3
// storage as a key-value database.
// This package uses [minio-go](https://github.com/minio/minio-go) S3 client.
package teos3

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const Version = "0.1.0"

const teoS3bucket = "teos3"

// TeoS3 methods receiver
type TeoS3 struct {
	context context.Context
	con     *minio.Client
	bucket  string
}

// Connect creates new cinnwction to S3 storage using accessKey, secretKey,
// endpoint, secure flag and bucket (if ommited then default 'teos3' buckets
// name used). The enpoind argument must be specified without http/https
// prefix(just domain and path), and the secure argument defines HTTPS if true
// or HTTP if false.
func Connect(accessKey, secretKey, endpoint string, secure bool,
	buckets ...string) (teos3 *TeoS3, err error) {

	teos3 = new(TeoS3)
	teos3.context = context.Background()

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

// SetContext sets context which will be used in all TeS3 operations if another
// context does not send in function call options argument.
func (m *TeoS3) SetContext(ctx context.Context) *TeoS3 {
	return m
}

// Set sets data to map by key. The options parameter may be ommited and
// than default SetObjectOptions with context.Background and empty
// minio.PutObjectOptions used.
func (m *TeoS3) Set(key string, data []byte, options ...*SetOptions) error {
	return m.SetObject(key, bytes.NewReader(data), int64(len(data)), options...)
}

// SetObject sets object to map by key. The options parameter may be ommited
// and than default SetObjectOptions with context.Background and empty
// minio.PutObjectOptions used.
func (m *TeoS3) SetObject(key string, reader io.Reader, objectSize int64,
	options ...*SetOptions) (err error) {

	// Set options
	opt := m.getSetOptions(options...)

	_, err = m.con.PutObject(opt.Context, m.bucket, key, reader, objectSize,
		minio.PutObjectOptions(opt.SetObjectOptions),
	)
	return
}

// Get map data by key. The options parameter may be ommited and than default
// GetObjectOptions with context.Background and empty minio.SetObjectOptions
// used.
func (m *TeoS3) Get(key string, options ...*GetOptions) (
	data []byte, err error) {

	// Get object
	obj, err := m.GetObject(key, options...)
	if err != nil {
		return
	}
	defer obj.Close()

	// Read from raw object
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(obj)
	if err != nil {
		return
	}

	data = buf.Bytes()

	return
}

// Get map object by key. The options parameter may be ommited and than default
// GetObjectOptions with context.Background and empty minio.SetObjectOptions
// used. Returned object must be cloused with obj.Close() after use.
func (m *TeoS3) GetObject(key string, options ...*GetOptions) (
	*minio.Object, error) {

	// Set options
	opt := m.getGetOptions(options...)

	return m.con.GetObject(opt.Context, m.bucket, key,
		minio.GetObjectOptions(opt.GetObjectOptions))
}

// Del remove key from map by key. The options parameter may be ommited and than
// default DelObjectOptions with context.Background and empty
// minio.RemoveObjectOptions used.
func (m *TeoS3) Del(key string, options ...*DelOptions) (err error) {

	// Set options
	opt := m.getDelOptions(options...)

	return m.con.RemoveObject(opt.Context, m.bucket, key,
		minio.RemoveObjectOptions(opt.DelObjectOptions))
}

// ListLen returns the number of records in the list by prefix and options.
func (m *TeoS3) ListLen(prefix string, options ...*ListOptions) int {

	// Get options from prefix and input options arguments
	opt := m.getListOptions(prefix, options...)

	objInfo := m.con.ListObjects(opt.Context, m.bucket,
		minio.ListObjectsOptions(opt.ListObjectsOptions))

	var i int
	for range objInfo {
		if opt.MaxKeys > 0 && i >= opt.MaxKeys {
			break
		}
		i++
	}
	return i
}

// List gets list of map keys by prefix. The options parameter may be ommited
// and than default ListObjectsOptions with context.Background and empty
// minio.ListObjectsOptions used. The Prefix parameter of the ListObjectsOptions
// will be always overwritten with the prefix functions argument (so it may be
// empty).
func (m *TeoS3) List(prefix string, options ...*ListOptions) (keys chan string) {

	// Get options from prefix and input options arguments
	opt := m.getListOptions(prefix, options...)

	// Get keys
	keys = make(chan string, 1)
	go func() {
		var i int

		objInfo := m.con.ListObjects(opt.Context, m.bucket,
			minio.ListObjectsOptions(opt.ListObjectsOptions))

		for obj := range objInfo {
			if opt.MaxKeys > 0 && i >= opt.MaxKeys {
				break
			}
			keys <- obj.Key
			i++
		}
		close(keys)
	}()

	return
}

// ListAr gets string array of map keys by prefix.
func (m *TeoS3) ListAr(prefix string, options ...*ListOptions) (
	list []string) {

	// Get options from prefix and input options arguments
	opt := m.getListOptions(prefix, options...)

	objInfo := m.con.ListObjects(opt.Context, m.bucket,
		minio.ListObjectsOptions(opt.ListObjectsOptions))

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
func (m *TeoS3) ListBody(prefix string, options ...*ListOptions) (
	mapDataChan chan MapData) {

	// Get options from prefix and input options arguments
	opt := m.getListOptions(prefix, options...)

	objInfo := m.con.ListObjects(opt.Context, m.bucket,
		minio.ListObjectsOptions(opt.ListObjectsOptions))

	mapDataChan = make(chan MapData, 1)
	go func() {
		var wg sync.WaitGroup

		for obj := range objInfo {
			wg.Add(1)
			go func(obj minio.ObjectInfo) {
				defer wg.Done()
				data, err := m.Get(obj.Key)
				if err != nil {
					return
				}
				mapDataChan <- MapData{obj.Key, data}
			}(obj)
		}

		wg.Wait()
		close(mapDataChan)
	}()

	return
}

// ListBodyAr gets MapData array with all keys and values by prefix.
func (m *TeoS3) ListBodyAr(prefix string, options ...*ListOptions) (
	listBody []MapData) {

	mapDataChan := m.ListBody(prefix, options...)

	for mapData := range mapDataChan {
		listBody = append(listBody, mapData)
	}

	return
}

// SetOptions contains context.Context and options specified by user for
// Set requests
type SetOptions struct {
	context.Context
	SetObjectOptions
}
type SetObjectOptions minio.PutObjectOptions

// NewSetOptions creates a new GetOptions object
func (m *TeoS3) NewSetOptions() *SetOptions { return &SetOptions{} }

// getSetOptions returns SetOptions created from input options arguments.
func (m *TeoS3) getSetOptions(options ...*SetOptions) (
	opt *SetOptions) {

	opt = &SetOptions{}
	if len(options) > 0 {
		opt = options[0]
	}

	if opt.Context == nil {
		opt.Context = m.context
	}

	return
}

// GetOptions contains context.Context and options are used to specify
// additional headers or options during GET requests.
type GetOptions struct {
	context.Context
	GetObjectOptions
}
type GetObjectOptions minio.GetObjectOptions

// NewGetOptions creates a new GetOptions object
func (m *TeoS3) NewGetOptions() *GetOptions { return &GetOptions{} }

// getGetOptions returns GetOptions created from input options arguments.
func (m *TeoS3) getGetOptions(options ...*GetOptions) (
	opt *GetOptions) {

	opt = &GetOptions{}
	if len(options) > 0 {
		opt = options[0]
	}

	if opt.Context == nil {
		opt.Context = m.context
	}

	return
}

// DelOptions contains context.Context and options for Remove
// requests.
type DelOptions struct {
	context.Context
	DelObjectOptions
}
type DelObjectOptions minio.RemoveObjectOptions

// NewDelOptions creates a new DelOptions object
func (m *TeoS3) NewDelOptions() *DelOptions { return &DelOptions{} }

// getDelOptions returns DelOptions created from input options arguments.
func (m *TeoS3) getDelOptions(options ...*DelOptions) (
	opt *DelOptions) {

	opt = &DelOptions{}
	if len(options) > 0 {
		opt = options[0]
	}

	if opt.Context == nil {
		opt.Context = m.context
	}

	return
}

// ListOptions contains context.Context and options for List requests.
type ListOptions struct {
	context.Context
	ListObjectsOptions
}
type ListObjectsOptions minio.ListObjectsOptions

// NewListOptions creates a new ListOptions object
func (m *TeoS3) NewListOptions() *ListOptions { return &ListOptions{} }

// SetMaxKeys sets MaxKeys list options value
func (l *ListOptions) SetMaxKeys(maxKeys int) *ListOptions {
	l.ListObjectsOptions.MaxKeys = maxKeys
	return l
}

// SetStartAfter sets StartAfter list options value
func (l *ListOptions) SetStartAfter(startAfter string) *ListOptions {
	l.ListObjectsOptions.StartAfter = startAfter
	return l
}

// getListOptions returns ListObjectsOptions created from input prefix and
// options arguments.
func (m *TeoS3) getListOptions(prefix string, options ...*ListOptions) (
	opt *ListOptions) {

	opt = &ListOptions{}
	if len(options) > 0 {
		opt = options[0]
	}

	if opt.Context == nil {
		opt.Context = m.context
	}
	opt.Prefix = prefix

	return
}
