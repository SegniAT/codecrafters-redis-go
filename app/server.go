package main

import (
	"fmt"
	"net"
	"os"
	"sync"
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
		buff := make([]byte, 30)

		_, err := conn.Read(buff)
		if err != nil {
			fmt.Println("Failed to read from connection")
			os.Exit(1)
		}

		conn.Write([]byte("+PONG\r\n"))
	}

}

func worker(wg *sync.WaitGroup, connChan chan net.Conn) {
	defer wg.Done()
	for conn := range connChan {
		handleConnection(conn)
	}
}
