package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/RESP"
)

var Handlers = map[string]func([]resp.Value) resp.Value{}

func (app *App) ping(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{Typ: resp.SIMPLE_STRING, Simple_str: []byte("PONG")}
	}

	return resp.Value{Typ: resp.BULK_STRING, Bulk_str: args[0].Bulk_str}
}

func (app *App) echo(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{
			Typ:          resp.BULK_STRING,
			Bulk_str_err: true,
		}
	}

	return resp.Value{
		Typ:      resp.BULK_STRING,
		Bulk_str: args[0].Bulk_str,
	}
}

func (app *App) set(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("wrong number of arguments"),
		}
	}

	key := string(args[0].Bulk_str)

	val := StoreVal{
		Val:       string(args[1].Bulk_str),
		StoreTime: time.Now(),
	}

	if len(args) >= 4 {
		opt1 := string(args[2].Bulk_str)
		if strings.ToLower(opt1) == "px" {
			expiry, err := strconv.Atoi(string(args[3].Bulk_str))
			if err != nil {
				return resp.Value{
					Typ:        resp.SIMPLE_ERROR,
					Simple_err: []byte("error trying to read expiry"),
				}
			}

			val.Exp = expiry
		}
	}

	app.mut.Lock()
	app.store[key] = val
	app.mut.Unlock()

	return resp.Value{
		Typ:        resp.SIMPLE_STRING,
		Simple_str: []byte("OK"),
	}
}

func (app *App) get(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{
			Typ:        resp.SIMPLE_ERROR,
			Simple_err: []byte("wrong number of arguments"),
		}
	}

	key := string(args[0].Bulk_str)

	app.mut.Lock()
	val, ok := app.store[key]
	app.mut.Unlock()

	if !ok {
		return resp.Value{
			Typ:  resp.NULL,
			Null: true,
		}
	}

	if val.Exp != 0 && time.Since(val.StoreTime).Milliseconds() > int64(val.Exp) {
		return resp.Value{
			Typ:          resp.BULK_STRING,
			Bulk_str_err: true,
		}
	}

	return resp.Value{
		Typ:      resp.BULK_STRING,
		Bulk_str: []byte(val.Val),
	}
}

func (app *App) info(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{
			Typ: resp.BULK_STRING,
		}
	}

	param := string(args[0].Bulk_str)

	switch param {
	case "replication":
		// role
		bulkString := ""
		if app.cfg.masterHost == "" {
			bulkString = fmt.Sprintf("%s%s", bulkString, "role:master")
		} else {
			bulkString = fmt.Sprintf("%s%s", bulkString, "role:slave")
		}

		masterReplIdStr := fmt.Sprintf("master_replid:%s", app.cfg.masterReplId)
		masterReplOffsetStr := fmt.Sprintf("master_repl_offset:%d", app.cfg.masterReplOffset)

		bulkString += " " + masterReplIdStr
		bulkString += " " + masterReplOffsetStr

		return resp.Value{
			Typ:      resp.BULK_STRING,
			Bulk_str: []byte(bulkString),
		}

	default:
		return resp.Value{
			Typ: resp.BULK_STRING,
		}
	}

}

func (app *App) psync(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{}
	}

	if string(args[0].Bulk_str) == "?" && string(args[1].Bulk_str) == "-1" {
		return resp.Value{
			Typ:        resp.SIMPLE_STRING,
			Simple_str: []byte(fmt.Sprintf("FULLRESYNC %s %d", app.cfg.masterReplId, app.cfg.masterReplOffset)),
		}
	}

	return resp.Value{}
}

func (app *App) emptyRdb(_ []resp.Value) resp.Value {
	emptyRdbHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	binaryData, err := hex.DecodeString(emptyRdbHex)
	if err != nil {
		fmt.Println("error decoding empty RDB hex value: ", emptyRdbHex)
		fmt.Println(err)
		return resp.Value{}
	}

	return resp.Value{
		Typ:          resp.BULK_STRING,
		Bulk_str_rdb: true,
		Bulk_str:     binaryData,
	}
}
