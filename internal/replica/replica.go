package replica

import (
	"fmt"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/RESP"
	"github.com/codecrafters-io/redis-starter-go/internal/config"
)

type Replica struct {
	ListeningPort int
	Conn          net.Conn
	Capabilities  []string
	AcceptedRDB   bool
}

func ConnectToMaster(cfg config.Config) (net.Conn, error) {
	connStr := fmt.Sprintf("%s:%v", cfg.MasterHost, cfg.MasterPort)
	conn, err := net.Dial("tcp", connStr)
	if err != nil {
		return nil, err
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
		return conn, fmt.Errorf("handshake (1/3): %v", err)
	}

	responseVal, err := respReader.Read()
	if err != nil {
		return conn, fmt.Errorf("handshake (1/3): %v", err)
	}

	if responseVal.Typ != resp.SIMPLE_STRING && strings.ToLower(string(responseVal.Simple_str)) != "ok" {
		return nil, fmt.Errorf("handshake (1/3) (didn't recieve OK from master): %v", err)
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
					Bulk_str: []byte(fmt.Sprintf("%d", cfg.Port)),
				},
			},
		})

	if err != nil {
		return conn, fmt.Errorf("handshake (2/3): %v", err)
	}

	responseVal, err = respReader.Read()
	if err != nil {
		return conn, fmt.Errorf("handshake (2/3): %v", err)
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
		return conn, fmt.Errorf("handshake (2/3): %v", err)
	}

	responseVal, err = respReader.Read()
	if err != nil {
		return conn, fmt.Errorf("handshake (2/3): %v", err)
	}

	if responseVal.Typ != resp.SIMPLE_STRING && strings.ToLower(string(responseVal.Simple_str)) != "ok" {
		return conn, fmt.Errorf("handshake (2/3) (didn't recieve OK from master): %v", err)
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
		return conn, fmt.Errorf("handshake (3/3) (couldn't write to master): %v", err)
	}

	responseVal, err = respReader.Read()
	if err != nil || responseVal.Typ != resp.SIMPLE_STRING || !strings.Contains(strings.ToUpper(responseVal.String()), "FULLRESYNC") {
		return conn, fmt.Errorf("handshake(3/3) (error recieving FULLRESYNC from master): %v", err)
	}

	// read the rdb file sent from master
	responseVal, err = respReader.ReadRDB()
	if err != nil {
		return conn, fmt.Errorf("handshake(3/3) (error recieving RDB file from master): %v", err)
	}

	return conn, nil
}
