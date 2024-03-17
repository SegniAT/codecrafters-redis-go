package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buff := make([]byte, 20)
	conn.Read(buff)

	buffStr := string(buff)
	switch {
	case buffStr == "*1\r\n$4\r\nping\r\n" || buffStr == "ping" || buffStr == "PING":
		conn.Write([]byte("+PONG\r\n"))
	}
}
