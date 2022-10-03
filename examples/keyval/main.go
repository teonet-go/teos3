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
	secure    = true
)

func main() {

	// Application logo
	fmt.Println(appName + " ver " + appVersion)

	// Application parameters
	flag.StringVar(&accessKey, "accesskey", accessKey, "S3 storage Access key")
	flag.StringVar(&secretKey, "secretkey", secretKey, "S3 storage Secret key")
	flag.StringVar(&endpoint, "endpoint", endpoint, "S3 storage Endpoint")
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
	log.Println("Connect")
	con, err := teos3.Connect(accessKey, secretKey, endpoint, secure)
	if err != nil {
		log.Fatalln(err)
	}

	// Set and Get records as Key Value
	const num = 100
	log.Println("Set and Get records")
	for i := 1; i <= num; i++ {
		// Key values to set and get
		key01 := fmt.Sprintf("/test/key-%02d", i)
		data01 := []byte(fmt.Sprintf("Hello %02d from TeoS3 Map!", i))

		// Set key to TeoS3 Map
		con.Map.Set(key01, data01)
		if err != nil {
			log.Fatalln(err)
		}

		// Get key from TeoS3 Map
		data, err := con.Map.Get(key01)
		if err != nil {
			log.Fatalln(err)
		}
		log.Println("Got data:", string(data))
	}

	// Get list of keys and print it
	log.Println("Get list of keys")
	list, err := con.Map.List("/test/key-")
	if err != nil {
		log.Fatalln(err)
	}
	for _, key := range list {
		fmt.Println(" ", key)
	}

	// Get keys from list asynchronously
	log.Println("Get keys from list asynchronously")
	var wg sync.WaitGroup
	var ch = make(chan []byte, num)
	for _, key := range list {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			data, err := con.Map.Get(key)
			if err != nil {
				log.Fatalln(err)
			}
			ch <- data
		}(key)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	for data := range ch {
		log.Println("Got data:", string(data))
	}

	// TODO: remove keys
	log.Println("Remove keys by keys in list")
	for _, key := range list {
		err := con.Map.Del(key)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println("  del", key)
	}

	log.Println("All done", time.Since(start))
}
