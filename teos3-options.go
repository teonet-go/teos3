// Copyright 2022-2023 Kirill Scherba <kirill@scherba.ru>.  All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The TeoS3 package option module.

package teos3

import (
	"context"

	"github.com/minio/minio-go/v7"
)

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

// GetInfoOptions contains context.Context and options are used to specify
// additional headers or options during GET Object Satat requests.
type GetInfoOptions struct {
	context.Context
	StatObjectOptions
}
type StatObjectOptions minio.StatObjectOptions

// NewGetStatOptions creates a new GetStatOptions object
func (m *TeoS3) NewGetStatOptions() *GetInfoOptions { return &GetInfoOptions{} }

// getGetInfoOptions returns GetInfoOptions created from input options arguments.
func (m *TeoS3) getGetInfoOptions(options ...*GetInfoOptions) (
	opt *GetInfoOptions) {

	opt = &GetInfoOptions{}
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

// CopyOptions contains context.Context for Copy or Move requests.
type CopyOptions struct {
	context.Context
}
