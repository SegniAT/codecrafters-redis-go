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

type Command struct {
	command    string
	args       []resp.Value
	marshaller resp.Writer
}

type Replica struct {
	listeningPort int
	conn          net.Conn
	capabilities  []string
	acceptedRDB   bool
}

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
	replicas map[string]Replica // "host:sentPort":Replica
	commands chan Command

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
		replicas: make(map[string]Replica),
		commands: make(chan Command),

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

	go app.handleCommands()

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
				fmt.Println("set: ", string(arr[1].Bulk_str), time.Now())
				app.commands <- Command{
					command:    "set",
					args:       arr,
					marshaller: *respMarshaller,
				}

			case "get":
				fmt.Println("get: ", string(arr[1].Bulk_str), time.Now())
				app.commands <- Command{
					command:    "get",
					args:       arr,
					marshaller: *respMarshaller,
				}
			case "info":
				response := app.info(args)
				respMarshaller.Write(response)
			case "replconf":
				if len(args) < 2 {
					respMarshaller.Write(resp.Value{
						Typ:        resp.SIMPLE_ERROR,
						Simple_err: []byte("insufficient arguments"),
					})
				}

				if string(args[0].Bulk_str) == "listening-port" {
					portStr := string(args[1].Bulk_str)
					port, err := strconv.Atoi(portStr)
					if err != nil {
						fmt.Println("error converting replconf listening_port to integer", err)
						continue
					}

					remoteAddr := conn.RemoteAddr().String()

					replica := Replica{
						listeningPort: port,
					}

					app.mut.Lock()
					app.replicas[remoteAddr] = replica
					app.mut.Unlock()
				}

				if string(args[0].Bulk_str) == "capa" {
					capability := string(args[1].Bulk_str)
					app.mut.Lock()
					replica := app.replicas[conn.RemoteAddr().String()]
					app.mut.Unlock()
					replica.capabilities = append(replica.capabilities, capability)
				}

				respMarshaller.Write(resp.Value{
					Typ:        resp.SIMPLE_STRING,
					Simple_str: []byte("OK"),
				})
			case "psync":
				response := app.psync(args)
				respMarshaller.Write(response)

				// send empty rdb
				response = app.emptyRdb(args)
				err := respMarshaller.Write(response)
				if err != nil {
					fmt.Println("Error sending empty RDB file to replica")
					continue
				}

				remoteAddr := conn.RemoteAddr().String()

				app.mut.Lock()
				replica := app.replicas[remoteAddr]
				replica.acceptedRDB = true
				replica.conn = conn
				app.replicas[remoteAddr] = replica
				app.mut.Unlock()
			default:
				return
			}
		} else {
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

	respReader := resp.NewRes(conn)
	respMarshaller := resp.NewWriter(conn)

	// handshake 1/3
	err = respMarshaller.Write(resp.Value{
		Typ: resp.ARRAY,
		Array: []resp.Value{
			{
				Typ:      resp.BULK_STRING,
				Bulk_str: []byte("ping"),
			},
		},
	})

	if err != nil {
		fmt.Println("handshake (1/3): ", err)
		os.Exit(1)
	}

	responseVal, err := respReader.Read()
	if err != nil {
		fmt.Println("handshake (1/3): ", err)
		os.Exit(1)
	}

	if responseVal.Typ != resp.SIMPLE_STRING && strings.ToLower(string(responseVal.Simple_str)) != "ok" {
		fmt.Println("handshake (1/3): didn't recieve OK from master")
		os.Exit(1)
	}

	// handshake 2/3
	err = respMarshaller.Write(
		resp.Value{
			Typ: resp.ARRAY,
			Array: []resp.Value{
				{
					Typ:      resp.BULK_STRING,
					Bulk_str: []byte("REPLCONF"),
				},
				{
					Typ:      resp.BULK_STRING,
					Bulk_str: []byte("listening-port"),
				},
				{
					Typ:      resp.BULK_STRING,
					Bulk_str: []byte(fmt.Sprintf("%d", app.cfg.port)),
				},
			},
		})

	if err != nil {
		fmt.Println("handshake (2/3): ", err)
		os.Exit(1)
	}

	responseVal, err = respReader.Read()
	if err != nil {
		fmt.Println("handshake (2/3): ", err)
		os.Exit(1)
	}

	err = respMarshaller.Write(
		resp.Value{
			Typ: resp.ARRAY,
			Array: []resp.Value{
				{
					Typ:      resp.BULK_STRING,
					Bulk_str: []byte("REPLCONF"),
				},
				{
					Typ:      resp.BULK_STRING,
					Bulk_str: []byte("capa"),
				},
				{
					Typ:      resp.BULK_STRING,
					Bulk_str: []byte("psync2"),
				},
			},
		})

	if err != nil {
		fmt.Println("handshake (2/3): ", err)
		os.Exit(1)
	}

	responseVal, err = respReader.Read()
	if err != nil {
		fmt.Println("handshake (2/3): ", err)
		os.Exit(1)
	}

	if responseVal.Typ != resp.SIMPLE_STRING && strings.ToLower(string(responseVal.Simple_str)) != "ok" {
		fmt.Println("handshake (2/3): didn't recieve OK from master")
		os.Exit(1)
	}

	// handshake 3/3
	err = respMarshaller.Write(
		resp.Value{
			Typ: resp.ARRAY,
			Array: []resp.Value{
				{
					Typ:      resp.BULK_STRING,
					Bulk_str: []byte("PSYNC"),
				},
				{
					Typ:      resp.BULK_STRING,
					Bulk_str: []byte("?"),
				},
				{
					Typ:      resp.BULK_STRING,
					Bulk_str: []byte("-1"),
				},
			},
		})

	if err != nil {
		fmt.Println("handshake (3/3) couldn't write to master: ", err)
		os.Exit(1)
	}

	// read the rdb file sent from master
	responseVal, err = respReader.Read()
	if err != nil {
		fmt.Println("handshake(3/3) error recieving RDB file from master: error ", err)
		os.Exit(1)
	}

	go handleConnection(conn, app)
}

func (app *App) propogate(response resp.Value) error {
	var err error

	for _, repl := range app.replicas {
		if repl.conn == nil {
			continue
		}
		go func(conn net.Conn) {
			respWriter := resp.NewWriter(conn)
			err = respWriter.Write(response)

		}(repl.conn)
	}

	return err
}

func (app *App) handleCommands() {
	for command := range app.commands {
		marshaller := command.marshaller

		switch command.command {
		case "get":
			respVal := app.get(command.args[1:])
			marshaller.Write(respVal)
		case "set":
			respVal := app.set(command.args[1:])
			marshaller.Write(respVal)

			if app.isMaster && len(app.replicas) > 0 {

				err := app.propogate(resp.Value{
					Typ:   resp.ARRAY,
					Array: command.args,
				})

				if err != nil {
					fmt.Println("failed to propogate to all replicas")
					fmt.Println(err)
				}
			}
		default:

		}
	}
}
