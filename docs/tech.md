# Technical comments for developers

## Hot to execute tests

### Temporally set STORJ credintals to the TEOS3 environment variables

```shell
export TEOS3_ACCESSKEY=$STORJS3_ACCESSKEY TEOS3_SECRETKEY=$STORJS3_SECRETKEY TEOS3_ENDPOINT=gateway.storjshare.io TEOS3_BUCKET=mysdocs
```

### The keyval example

The 'keyval' example executes following tasks:

- sets some numer of data records (objects) by key (objects name) to the key/value db baset on s3 bucket;

- gets list of saved data records by prefix;

- gets some numer of data records (objects) by key (objects name) from the key/value db baset on s3 bucket;

- deletes all saved data records (objects) by key (object name) from the key/value database in the s3 bucket.

All these tasks are performed in parallel mode.

```shell
# run the keyval example
go run ./examples/keyval/
```
