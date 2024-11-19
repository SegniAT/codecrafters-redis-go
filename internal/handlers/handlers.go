package handlers

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/RESP"
	"github.com/codecrafters-io/redis-starter-go/internal/config"
	"github.com/codecrafters-io/redis-starter-go/internal/replica"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

const (
	PING     = "PING"
	PONG     = "PONG"
	OK       = "OK"
	ECHO     = "ECHO"
	SET      = "SET"
	GET      = "GET"
	INFO     = "INFO"
	PSYNC    = "PSYNC"
	REPLCONF = "REPLCONF"
	WAIT     = "WAIT"

	PX             = "PX"
	REPLICATION    = "REPLICATION"
	LISTENING_PORT = "LISTENING-PORT"
	CAPA           = "CAPA"
	FULLRESYNC     = "FULLRESYNC"
	ACK            = "ACK"
	GETACK         = "GETACK"
)

func ClientHandler(respVal resp.Value, conn net.Conn, replicas *map[string]*replica.Replica, store *store.Store, cfg *config.Config, replicasMut *sync.RWMutex, ackRecieved *chan bool) ([]*resp.Value, error) {
	arr := respVal.Array
	command := strings.ToUpper(arr[0].String())
	args := arr[1:]

	fmt.Println("\n▶️ main handler: ", respVal.String())

	var response []*resp.Value
	switch command {
	case PING:
		response = append(response, ping(args))
	case ECHO:
		response = append(response, echo(args))
	case SET:
		setResponse := set(args, store)
		response = append(response, setResponse)
		if strings.ToUpper(setResponse.String()) == "OK" {
			length := 0
			for _, r := range arr {
				length += len(r.Bulk_str)
			}
			cfg.MasterReplOffset += length
		}
		err := replicate(respVal, replicas)

		if err != nil {
			return response, fmt.Errorf("error replicating 'SET' command: %v", err)
		}
		fmt.Println("✅'SET' replicated successfully")
	case GET:
		response = append(response, get(args, store))
	case INFO:
		response = append(response, info(args, *cfg))
	case PSYNC:
		response = append(response, psync(args, *cfg))

		// send empty rdb
		emptyRdb := EmptyRdb()
		response = append(response, emptyRdb)

		remoteAddr := conn.RemoteAddr().String()
		replicasMut.Lock()
		(*replicas)[remoteAddr].AcceptedRDB = true
		(*replicas)[remoteAddr].Conn = conn
		replicasMut.Unlock()
	case REPLCONF:
		res := replConf(args, conn, replicas, replicasMut, ackRecieved)
		response = append(response, res)
	case WAIT:
		response = append(response, wait(args, replicas, cfg, ackRecieved))
	default:
		response = append(response, &resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("Unknown command sent from client"),
		})
		return response, fmt.Errorf("Unknown command sent from client")
	}

	return response, nil
}

func MasterReplicaConnHandler(respVal resp.Value, replicas *map[string]*replica.Replica, store *store.Store, cfg *config.Config, replicasMut *sync.RWMutex) ([]resp.Value, error) {
	arr := respVal.Array
	command := strings.ToUpper(arr[0].String())
	args := arr[1:]

	fmt.Println("\nmaster 🔁 replica handler: ", respVal.String())

	var response []resp.Value
	switch command {
	case PING:
		ping(args)
	case SET:
		set(args, store)
	case REPLCONF:
		arg1 := strings.ToUpper(args[0].String())
		masterReplicaOffset := strconv.Itoa(cfg.MasterReplOffset)

		if arg1 == GETACK {
			resp := resp.Value{
				Typ: resp.ARRAY,
				Array: []resp.Value{
					{
						Typ:      resp.BULK_STRING,
						Bulk_str: []byte("REPLCONF"),
					},
					{
						Typ:      resp.BULK_STRING,
						Bulk_str: []byte("ACK"),
					},
					{
						Typ:      resp.BULK_STRING,
						Bulk_str: []byte(masterReplicaOffset),
					},
				},
			}
			response = append(response, resp)
		} else {
			return response, fmt.Errorf("Unknown REPLCONF parameter sent from master to replica")
		}
	default:
		response = append(response, resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("Unknown command sent from master to replica"),
		})
		return response, fmt.Errorf("Unknown command sent from master to replica")
	}

	rawBytes := respVal.Marshal()
	cfg.MasterReplOffset += len(rawBytes)

	return response, nil
}

func ping(args []resp.Value) *resp.Value {
	if len(args) == 0 {
		return &resp.Value{Typ: resp.SIMPLE_STRING, Simple_str: []byte("PONG")}
	}

	return &resp.Value{Typ: resp.BULK_STRING, Bulk_str: args[0].Bulk_str}
}

func echo(args []resp.Value) *resp.Value {
	if len(args) == 0 {
		return &resp.Value{
			Typ:          resp.BULK_STRING,
			Bulk_str_err: true,
		}
	}

	return &resp.Value{
		Typ:      resp.BULK_STRING,
		Bulk_str: args[0].Bulk_str,
	}
}

func set(args []resp.Value, s *store.Store) *resp.Value {
	if len(args) < 2 {
		return &resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("wrong number of arguments"),
		}
	}

	key := args[0].String()

	val := store.StoreVal{
		Val: args[1].String(),
	}

	if len(args) >= 4 {
		opt1 := strings.ToUpper(args[2].String())
		if opt1 == PX {
			expiry, err := time.ParseDuration(args[3].String() + "ms")
			fmt.Println("expiry ms: ", expiry.Milliseconds())
			if err != nil {
				return &resp.Value{
					Typ:        resp.SIMPLE_ERROR,
					Simple_err: []byte("error trying to read expiry"),
				}
			}

			val.StoredAt = time.Now()
			val.ExpiresAfter = expiry
		}
	}

	s.Set(key, &val)

	return &resp.Value{
		Typ:        resp.SIMPLE_STRING,
		Simple_str: []byte("OK"),
	}
}

func get(args []resp.Value, s *store.Store) *resp.Value {
	if len(args) == 0 {
		return &resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("wrong number of arguments"),
		}
	}

	key := args[0].String()

	val, ok := s.Get(key)

	if !ok {
		return &resp.Value{
			Typ:  resp.NULL,
			Null: true,
		}
	}

	if val.ExpiresAfter != 0 && time.Since(val.StoredAt).Milliseconds() > val.ExpiresAfter.Milliseconds() {
		return &resp.Value{
			Typ:          resp.BULK_STRING,
			Bulk_str_err: true,
		}
	}

	return &resp.Value{
		Typ:      resp.BULK_STRING,
		Bulk_str: []byte(val.Val),
	}
}

func info(args []resp.Value, cfg config.Config) *resp.Value {
	if len(args) == 0 {
		return &resp.Value{
			Typ: resp.BULK_STRING,
		}
	}

	param := strings.ToUpper(args[0].String())

	switch param {
	case REPLICATION:
		bulkString := ""
		if cfg.MasterHost == "" {
			bulkString = fmt.Sprintf("%s%s", bulkString, "role:master")
		} else {
			bulkString = fmt.Sprintf("%s%s", bulkString, "role:slave")
		}

		masterReplIdStr := fmt.Sprintf("master_replid:%s", cfg.MasterReplId)
		masterReplOffsetStr := fmt.Sprintf("master_repl_offset:%d", cfg.MasterReplOffset)

		bulkString += " " + masterReplIdStr
		bulkString += " " + masterReplOffsetStr

		return &resp.Value{
			Typ:      resp.BULK_STRING,
			Bulk_str: []byte(bulkString),
		}

	default:
		return &resp.Value{
			Typ: resp.BULK_STRING,
		}
	}

}

func psync(args []resp.Value, cfg config.Config) *resp.Value {
	if len(args) < 2 {
		return &resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("not enough arguments for the command 'psync'"),
		}
	}

	if args[0].String() == "?" && args[1].String() == "-1" {
		return &resp.Value{
			Typ:        resp.SIMPLE_STRING,
			Simple_str: []byte(fmt.Sprintf("FULLRESYNC %s %d", cfg.MasterReplId, cfg.MasterReplOffset)),
		}
	}

	return nil
}

func EmptyRdb() *resp.Value {
	emptyRdbHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	binaryData, err := hex.DecodeString(emptyRdbHex)
	if err != nil {
		fmt.Println("error decoding empty RDB hex value: ", emptyRdbHex)
		fmt.Println(err)
		return nil
	}

	return &resp.Value{
		Typ:          resp.BULK_STRING,
		Bulk_str_rdb: true,
		Bulk_str:     binaryData,
	}
}

func replConf(args []resp.Value, conn net.Conn, replicas *map[string]*replica.Replica, mut *sync.RWMutex, ackRecieved *chan bool) *resp.Value {
	if len(args) < 2 {
		return &resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("insufficient arguments"),
		}
	}

	arg1 := strings.ToUpper(args[0].String())
	arg2 := strings.ToUpper(args[1].String())

	switch arg1 {
	case LISTENING_PORT:
		portStr := arg2
		port, err := strconv.Atoi(portStr)
		if err != nil {
			fmt.Println("error converting replconf listening-port to integer", err)

			return &resp.Value{
				Typ:        resp.SIMPLE_ERROR,
				Simple_err: []byte("invalid listening-port argument"),
			}
		}

		remoteAddr := conn.RemoteAddr().String()

		replica := &replica.Replica{
			ListeningPort: port,
		}

		mut.Lock()
		(*replicas)[remoteAddr] = replica
		mut.Unlock()

		return &resp.Value{
			Typ:        resp.SIMPLE_STRING,
			Simple_str: []byte("OK"),
		}
	case CAPA:
		capability := arg2
		mut.Lock()
		replica := (*replicas)[conn.RemoteAddr().String()]
		mut.Unlock()
		replica.Capabilities = append(replica.Capabilities, capability)

		return &resp.Value{
			Typ:        resp.SIMPLE_STRING,
			Simple_str: []byte("OK"),
		}
	case ACK:
		(*ackRecieved) <- true
		return nil
	default:
		return &resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("invalid argument for 'REPLCONF' command"),
		}
	}

}

func wait(args []resp.Value, replicas *map[string]*replica.Replica, _ *config.Config, ackRecieved *chan bool) *resp.Value {
	if len(args) < 2 {
		return &resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("invalid number of arguments for 'WAIT' command"),
		}
	}

	count, err := strconv.Atoi(args[0].String())
	if err != nil {
		return &resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("invalid type of argument for 'WAIT' command"),
		}
	}

	// ms
	timeout, err := time.ParseDuration(args[1].String() + "ms")
	if err != nil {
		return &resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("invalid type of argument for 'WAIT' command"),
		}
	}

	getAckCommand := resp.Value{
		Typ: resp.ARRAY,
		Array: []resp.Value{
			{
				Typ:      resp.BULK_STRING,
				Bulk_str: []byte("REPLCONF"),
			},
			{
				Typ:      resp.BULK_STRING,
				Bulk_str: []byte("GETACK"),
			},
			{
				Typ:      resp.BULK_STRING,
				Bulk_str: []byte("*"),
			},
		},
	}

	timer := time.After(timeout * time.Millisecond)

	acks := 0

	for key, repl := range *replicas {
		if repl.Offset > 0 {
			respReader := resp.NewRes(repl.Conn)
			respMarshaller := resp.NewWriter(repl.Conn)

			bytesWritten, err := respMarshaller.Write(getAckCommand)
			if err != nil {
				fmt.Println("error sending 'REPLCONF GETACK *' command to replica")
				continue
			}
			repl.Offset += bytesWritten

			fmt.Println("GETACK sent to: ", key)
			go func(key string, repl *replica.Replica) {
				fmt.Printf("wating for response from the replica %s for 'GETACK' command...\n", key)
				_, err = respReader.Read()

				if err != nil {
					fmt.Println("error recieving response for 'GETACK' command from replica: ", key, err)
				} else {
					fmt.Println("got response for 'GETACK' command from replica: ", key)
				}
				(*ackRecieved) <- true
			}(key, repl)
		} else {
			acks++
		}
	}

outer:
	for acks < count {
		fmt.Println("(loop) acks: ", acks)
		select {
		case <-(*ackRecieved):
			acks++
			fmt.Println("ackRecieved: acks=", acks)
		case <-timer:
			fmt.Println("timeout: acks=", acks)
			break outer
		}
	}

	respAcks := resp.Value{
		Typ:     resp.INTEGER,
		Integer: int64(acks),
	}
	fmt.Println("returned acks: ", respAcks)

	return &respAcks
}

func replicate(response resp.Value, replicas *map[string]*replica.Replica) error {
	var err error
	var wg sync.WaitGroup

	wg.Add(len(*replicas))
	for key, repl := range *replicas {
		go func(conn net.Conn, key string, repl *replica.Replica, replicas *map[string]*replica.Replica, err *error) {
			defer wg.Done()
			if repl.Conn == nil {
				delete(*replicas, key)
				return
			}
			respWriter := resp.NewWriter(conn)
			bytesWritten, respErr := respWriter.Write(response)
			*err = respErr

			if respErr != nil {
				// remove stale replica
				delete(*replicas, key)
			} else {
				repl.Offset += bytesWritten
			}
		}(repl.Conn, key, repl, replicas, &err)
	}
	wg.Wait()

	return err
}
