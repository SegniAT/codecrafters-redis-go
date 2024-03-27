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

type StoreVal struct {
	Val       string
	StoreTime time.Time
	Exp       int // ms
}

type Config struct {
	port             int
	masterHost       string
	masterPort       int
	masterReplId     string
	masterReplOffset int
}

type App struct {
	isMaster bool
	cfg      Config
	handlers map[string]func([]resp.Value) resp.Value
	store    map[string]StoreVal

	mut sync.Mutex
}

func main() {
	var cfg Config

	flag.IntVar(&cfg.port, "port", 6379, "Port number to expose the server to")
	replicaOf := flag.String("replicaof", "", "Specify the master host and port in format: <MASTER_HOST> <MASTER_PORT>")

	flag.Parse()

	app := &App{
		isMaster: *replicaOf == "",
		cfg:      cfg,
		handlers: Handlers,
		store:    make(map[string]StoreVal),

		mut: sync.Mutex{},
	}

	if !app.isMaster {
		argsLen := len(os.Args)
		for ind, arg := range os.Args {
			if arg == "--replicaof" {
				if ind+2 >= argsLen {
					fmt.Println("not enough arguments for --replicaof: <MASTER_HOST> <MASTER_PORT>")
					os.Exit(1)
				}
				app.cfg.masterHost = os.Args[ind+1]
				num, err := strconv.Atoi(os.Args[ind+2])
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				app.cfg.masterPort = num
			}
		}

	}

	app.cfg.masterReplId = RandString(40)
	app.cfg.masterReplOffset = 0

	if !app.isMaster {
		connectToMaster(app)
	}

	connStr := fmt.Sprintf("0.0.0.0:%v", cfg.port)
	listener, err := net.Listen("tcp", connStr)
	defer listener.Close()
	if err != nil {
		fmt.Println("Failed to bind to port: ", cfg.port)
		os.Exit(1)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleConnection(conn, app)
	}

}

func handleConnection(conn net.Conn, app *App) {
	defer conn.Close()

	respReader := resp.NewRes(conn)
	respMarshaller := resp.NewWriter(conn)

	for {
		respVal, err := respReader.Read()

		if err != nil {
			fmt.Println("Failed to read from connection: ", err)
			return
		}

		if respVal.Typ == resp.ARRAY {
			arr := respVal.Array

			if len(arr) < 1 {
				return
			}

			command := strings.ToLower(string(arr[0].Bulk_str))
			args := arr[1:]
			switch command {
			case "echo":
				response := app.echo(args)
				respMarshaller.Write(response)
			case "ping":
				response := app.ping(args)
				respMarshaller.Write(response)
			case "set":
				response := app.set(args)
				respMarshaller.Write(response)
			case "get":
				response := app.get(args)
				respMarshaller.Write(response)
			case "info":
				response := app.info(args)
				respMarshaller.Write(response)
			default:
				return
			}
		} else {
			fmt.Println("what tha heeeell: ", respVal)
		}

	}

}

func connectToMaster(app *App) {
	connStr := fmt.Sprintf("%s:%v", app.cfg.masterHost, app.cfg.masterPort)
	conn, err := net.Dial("tcp", connStr)
	if err != nil {
		fmt.Println("Failed to connect to master: ", connStr)
		os.Exit(1)
	}

	// handshake with master
	respMarshaller := resp.NewWriter(conn)

	respMarshaller.Write(resp.Value{
		Typ: resp.ARRAY,
		Array: []resp.Value{
			{
				Typ:      resp.BULK_STRING,
				Bulk_str: []byte("ping"),
			},
		},
	})

	go handleConnection(conn, app)
}
