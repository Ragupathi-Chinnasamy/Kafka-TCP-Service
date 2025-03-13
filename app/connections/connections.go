package connections

import (
	"net"
	"sync"
)

var connections sync.Map

func AddConnection(id string, conn net.Conn) {
	connections.Store(id, conn)
}

func GetConnection(id string) (net.Conn, bool) {

	conn, exists := connections.Load(id)
	if conn == nil {
		return nil, false
	}

	return conn.(net.Conn), exists
}

func RemoveConnection(id string) {
	connections.Delete(id)
}
