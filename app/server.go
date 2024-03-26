package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/RESP"
)

const WORKERS = 15

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
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

	tempHash := make(map[string]string)

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
			fmt.Println(command)
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
				if len(arr) != 3 {
					respMarhaller.Write(resp.Value{
						Typ:        resp.SIMPLE_ERROR,
						Simple_err: []byte("wrong number of arguments"),
					})
				}
				key := string(arr[1].Bulk_str)
				val := string(arr[2].Bulk_str)

				tempHash[key] = val
				respMarhaller.Write(resp.Value{
					Typ:        resp.SIMPLE_STRING,
					Simple_str: []byte("OK"),
				})
				fmt.Println(tempHash)
			case "get":
				if len(arr) != 2 {
					respMarhaller.Write(resp.Value{
						Typ:        resp.SIMPLE_ERROR,
						Simple_err: []byte("wrong number of arguments"),
					})
				}

				key := string(arr[1].Bulk_str)
				val, ok := tempHash[key]
				if !ok {
					respMarhaller.Write(resp.Value{
						Typ:        resp.SIMPLE_ERROR,
						Simple_err: []byte(fmt.Sprintf("value for key %s not found", key)),
					})
				}

				respMarhaller.Write(resp.Value{
					Typ:      resp.BULK_STRING,
					Bulk_str: []byte(val),
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
