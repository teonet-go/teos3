# `TeoS3` package and `s3cp` utilite

The TeoS3 package contains Golang features that make it easy to use S3 storage
as a key-value database.

This project contain also the `s3cp` utilite which copy files from disk to s3
storage and back

[![GoDoc](https://godoc.org/github.com/teonet-go/teos3?status.svg)](https://godoc.org/github.com/teonet-go/teos3/)
[![Go Report Card](https://goreportcard.com/badge/github.com/teonet-go/teos3)](https://goreportcard.com/report/github.com/teonet-go/teos3)

-----------------------

## Using `s3cp` utilite

### Install

To install `s3cp` application use next command:

```shell
go install github.com/teonet-go/teos3/cmd/s3cp
```

### Description

The `s3cp` application copy file to/from S3 storage.

The S3 storage credentials may be set in application parameters or in
environment variables:

```shell
TEOS3_ACCESSKEY -- S3 storage Access key
TEOS3_SECRETKEY -- S3 storage Secret key
TEOS3_ENDPOINT  -- S3 storage Endpoint
TEOS3_BUCKET    -- S3 storage Bucket
```

Parameters and arguments:

```shell
s3cp [OPTION] source target
use s3:/folder_and_object_name to define S3 in source or target

Usage of /tmp/go-build1982013444/b001/exe/s3cp:

-accesskey string
  S3 storage Access key
-bucket string
  S3 storage Bucket
-endpoint string
  S3 storage Endpoint
-secretkey string
  S3 storage Secret key
-secure
  set secure=false to enable insecure (HTTP) access (default true)
```

### Logs

The `s3cp` application sends logs to syslog. To read current log messages in
`archlinux` use `journalctl -f` command.

-----------------------

## `TeoS3` package description

The `teos3` package contains Golang functions to rasy use S3 storage as
KeyValue Database. There is functions to Set, Get, GetList using string key with any data value.

You can find complete packets documentation at: <https://pkg.go.dev/github.com/teonet-go/teos3>

### The `keyval` example

There is basic [keyval](examples/keyval/main.go) example which used most of packets functions.

This example executes following tasks:

- sets some numer of data records (objects) by key (objects name) to the key/value db baset on s3 bucket;

- gets list of saved data records by prefix;

- gets some numer of data records (objects) by key (objects name) to the key/value db baset on s3 bucket;

- deletes all saved data records (objects) by key (object name) from the key/value database in the s3 bucket.

All this tasks are performed in parallel mode.

Fill next environment variables to run this example:

```shell
export TEOS3_ACCESSKEY=YOUR_ACCESSKEY TEOS3_SECRETKEY=YOUR_SECRETKEY TEOS3_ENDPOINT=YOUR_GATEWAT TEOS3_BUCKET=YOUR_BUCKET
```

Use next command to run this example:

```shell
go run ./examples/keyval/
```

There is a part of code with connect, set and get key value:

```go
// Connect to S3 storage
con, err := teos3.Connect(accessKey, secretKey, endpoint, secure)
if err != nil {
    log.Fatalln(err)
}

// Set key to teos3 Map
err = con.Map.Set(key, data)
if err != nil {
    log.Fatalln(err)
}

// Get key from TeoS3 Map
data, err := con.Map.Get(key)
if err != nil {
    log.Fatalln(err)
}
```

-----------------------

## Licence

[BSD](LICENSE)
