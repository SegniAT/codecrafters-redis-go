package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/RESP"
)

const WORKERS = 15

func main() {
	PORT := flag.Int("port", 6379, "port number to expose the server to")
	flag.Parse()

	connStr := fmt.Sprintf("0.0.0.0:%v", *PORT)

	listener, err := net.Listen("tcp", connStr)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

	var wg sync.WaitGroup
	connsChan := make(chan net.Conn, 20)

	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		go worker(&wg, connsChan)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go func() {
			connsChan <- conn
		}()
	}

}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	type StoreVal struct {
		Val       string
		StoreTime time.Time
		Exp       int // ms
	}

	tempHash := make(map[string]StoreVal)

	for {
		respReader := resp.NewRes(conn)
		respVal, err := respReader.Read()

		if err != nil {
			fmt.Println("Failed to read from connection: ", err)
			break
		}

		respMarhaller := resp.NewWriter(conn)

		if respVal.Typ == resp.ARRAY {
			arr := respVal.Array

			if len(arr) < 1 {
				return
			}

			command := strings.ToLower(string(arr[0].Bulk_str))
			switch command {
			case "echo":
				var echoResp []byte
				if len(arr) > 1 {
					echoResp = arr[1].Bulk_str
				}
				respMarhaller.Write(resp.Value{Typ: resp.SIMPLE_STRING, Simple_str: echoResp})
			case "ping":
				respMarhaller.Write(resp.Value{Typ: resp.SIMPLE_STRING, Simple_str: []byte("PONG")})
			case "set":
				if len(arr) < 3 {
					respMarhaller.Write(resp.Value{
						Typ:        resp.SIMPLE_ERROR,
						Simple_err: []byte("wrong number of arguments"),
					})
				}
				key := string(arr[1].Bulk_str)

				val := StoreVal{
					Val:       string(arr[2].Bulk_str),
					StoreTime: time.Now(),
				}

				if len(arr) >= 5 {
					opt1 := string(arr[3].Bulk_str)
					if strings.ToLower(opt1) == "px" {
						expiry, err := strconv.Atoi(string(arr[4].Bulk_str))
						if err != nil {
							respMarhaller.Write(resp.Value{
								Typ:        resp.SIMPLE_ERROR,
								Simple_err: []byte("error trying to read expiry"),
							})
						}

						val.Exp = expiry
					}
				}

				tempHash[key] = val
				respMarhaller.Write(resp.Value{
					Typ:        resp.SIMPLE_STRING,
					Simple_str: []byte("OK"),
				})
			case "get":
				if len(arr) != 2 {
					respMarhaller.Write(resp.Value{
						Typ:        resp.SIMPLE_ERROR,
						Simple_err: []byte("wrong number of arguments"),
					})
				}

				key := string(arr[1].Bulk_str)
				val, ok := tempHash[key]

				if val.Exp != 0 && time.Since(val.StoreTime).Milliseconds() > int64(val.Exp) {
					conn.Write(resp.NullBulkString())
					continue
				}

				if !ok {
					respMarhaller.Write(resp.Value{
						Typ:  resp.NULL,
						Null: true,
					})
				}

				respMarhaller.Write(resp.Value{
					Typ:      resp.BULK_STRING,
					Bulk_str: []byte(val.Val),
				})

			default:
				break
			}
		}

	}

}

func worker(wg *sync.WaitGroup, connChan chan net.Conn) {
	defer wg.Done()
	for conn := range connChan {
		handleConnection(conn)
	}
}
