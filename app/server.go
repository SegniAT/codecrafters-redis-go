package main

import (
	"flag"
	"fmt"
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
					fmt.Println(err)
					os.Exit(1)
				}
				app.cfg.MasterPort = num
			}
		}

	}

	app.cfg.MasterReplId = RandString(40)
	app.cfg.MasterReplOffset = 0

	if !app.isMaster {
		conn, err := replica.ConnectToMaster(app.cfg)
		if err != nil && conn == nil {
			fmt.Println("error connecting to master: ", err)
			os.Exit(1)
		}

		go handleConnection(conn, app, true)
	}

	connStr := fmt.Sprintf("0.0.0.0:%v", cfg.Port)

	listener, err := net.Listen("tcp", connStr)
	defer listener.Close()
	if err != nil {
		fmt.Println("Failed to bind to port: ", cfg.Port)
		os.Exit(1)
	}

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

		if err != nil {
			fmt.Println("Failed to read from connection: ", err)
			return
		}

		if respVal.Typ == resp.ARRAY {
			arr := respVal.Array

			if len(arr) < 1 {
				return
			}

			responses := handlers.Handler(respVal, conn, app.replicas, app.store, app.cfg, &app.mut)

			command := strings.ToUpper(arr[0].String())
			args := arr[1:]
			fmt.Println("command: ", respVal.String())

			// if slave connection to master, only respond to REPLCONF GETACK
			if !fromMaster || (command == handlers.REPLCONF && len(args) > 1 && args[0].String() == handlers.GETACK) {
				for _, response := range responses {
					respMarshaller.Write(response)
				}
			}
		}
	}

}
