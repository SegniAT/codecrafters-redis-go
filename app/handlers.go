package main

import (
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

	app.store[key] = val
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
	val, ok := app.store[key]

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
