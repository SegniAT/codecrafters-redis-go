package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/RESP"
	"github.com/codecrafters-io/redis-starter-go/internal/config"
	"github.com/codecrafters-io/redis-starter-go/internal/handlers"
	"github.com/codecrafters-io/redis-starter-go/internal/replica"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

type Command struct {
	command      string
	args         []resp.Value
	marshaller   resp.Writer
	connToMaster bool
}
type App struct {
	isMaster bool
	cfg      config.Config
	store    *store.Store
	replicas map[string]replica.Replica // "host:sentPort":Replica

	mut sync.RWMutex
}

func main() {
	var cfg config.Config

	flag.IntVar(&cfg.Port, "port", 6379, "Port number to expose the server to")
	replicaOf := flag.String("replicaof", "", "Specify the master host and port in format: <MASTER_HOST> <MASTER_PORT>")

	flag.Parse()
	app := &App{
		isMaster: *replicaOf == "",
		cfg:      cfg,
		store:    store.NewStore(),
		replicas: make(map[string]replica.Replica),

		mut: sync.RWMutex{},
	}

	if !app.isMaster {
		argsLen := len(os.Args)
		for ind, arg := range os.Args {
			if arg == "--replicaof" {
				if ind+2 >= argsLen {
					fmt.Println("not enough arguments for --replicaof: <MASTER_HOST> <MASTER_PORT>")
					os.Exit(1)
				}
				app.cfg.MasterHost = os.Args[ind+1]
				num, err := strconv.Atoi(os.Args[ind+2])
				if err != nil {
					fmt.Println("couldn't convert master port to number: ", err)
					os.Exit(1)
				}
				app.cfg.MasterPort = num
			}
		}

	}

	app.cfg.MasterReplId = RandString(40)
	app.cfg.MasterReplOffset = 0

	if !app.isMaster {
		go app.ConnectToMaster()
	}

	connStr := fmt.Sprintf("0.0.0.0:%v", cfg.Port)
	listener, err := net.Listen("tcp", connStr)
	if err != nil {
		fmt.Println("Failed to bind to port: ", cfg.Port)
		os.Exit(1)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleConnection(conn, app, false)
	}

}

func handleConnection(conn net.Conn, app *App, fromMaster bool) {
	defer conn.Close()

	respReader := resp.NewRes(conn)
	respMarshaller := resp.NewWriter(conn)

	for {
		respVal, err := respReader.Read()
		if errors.Is(err, io.EOF) {
			fmt.Println("CONNECTION CLOSED: EOF")
			break
		}

		if err != nil {
			fmt.Println("Failed to read from connection: ", err)
			break
		}

		if respVal.Typ == resp.ARRAY {
			arr := respVal.Array

			if len(arr) < 1 {
				return
			}

			if !fromMaster {
				responses := handlers.ClientHandler(respVal, conn, app.replicas, app.store, app.cfg, &app.mut)

				for _, response := range responses {
					respMarshaller.Write(response)
				}
			} else {
				responses, err := handlers.MasterReplicaConnHandler(respVal, conn, app.replicas, app.store, app.cfg, &app.mut)
				if err != nil {
					fmt.Println("Error MasterReplicaConnHandler: ", err)
					continue
				}

				for _, response := range responses {
					respMarshaller.Write(response)
				}
			}

		}
	}

}

func (app *App) ConnectToMaster() {
	connStr := fmt.Sprintf("%s:%v", app.cfg.MasterHost, app.cfg.MasterPort)
	conn, err := net.Dial("tcp", connStr)
	defer conn.Close()
	if err != nil {
		fmt.Println("Error dialing master at: ", connStr, err)
		os.Exit(1)
	}

	respReader := resp.NewRes(conn)
	respMarshaller := resp.NewWriter(conn)

	// handshake 1/3
	// expecting +PONG\r\n back
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
		fmt.Println("handshake (1/3) (didn't recieve OK from master): ", err)
		os.Exit(1)
	}

	// handshake 2/3
	// expecting +OK\r\n back
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
					Bulk_str: []byte(fmt.Sprintf("%d", app.cfg.Port)),
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

	// expecting +OK\r\n back
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
		fmt.Println("handshake (2/3) (didn't recieve OK from master): ", err)
		os.Exit(1)
	}

	// handshake 3/3
	// expecting +FULLRESYNC <REPL_ID> 0\r\n back
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
		fmt.Println("handshake (3/3) (couldn't write to master): ", err)
		os.Exit(1)
	}

	responseVal, err = respReader.Read()
	if err != nil || responseVal.Typ != resp.SIMPLE_STRING || !strings.Contains(strings.ToUpper(responseVal.String()), "FULLRESYNC") {
		fmt.Println("handshake(3/3) (error recieving FULLRESYNC from master): ", err)
		os.Exit(1)
	}

	fmt.Println("connectToMaster (responseVal FULLRESYNC): ", responseVal)

	// read the rdb file sent from master
	responseVal, err = respReader.ReadRDB()
	if err != nil {
		fmt.Println("handshake(3/3) (error recieving RDB file from master): ", err)
		os.Exit(1)
	}

	fmt.Println("connectToMaster (responseVal RDB): ", responseVal)

	for {
		responseVal, err = respReader.Read()

		if errors.Is(err, io.EOF) {
			fmt.Println("CONNECTION CLOSED: EOF")
			break
		}
		if err != nil {
			fmt.Println("Error reading from connection")
			break
		}

		responses, err := handlers.MasterReplicaConnHandler(responseVal, conn, app.replicas, app.store, app.cfg, &app.mut)
		if err != nil {
			fmt.Println("MasterReplicaHandler: ", err)
		}

		for _, r := range responses {
			respMarshaller.Write(r)
		}
	}
}
