package replica

import (
	"net"
)

type Replica struct {
	ListeningPort int
	Conn          net.Conn
	Capabilities  []string
	AcceptedRDB   bool
}
