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
