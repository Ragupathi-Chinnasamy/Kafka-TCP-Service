package tcp

import (
	"go-kafka-tcp-service/app/connections"
	"go-kafka-tcp-service/app/messenger"
	"go-kafka-tcp-service/common/config"
	"go-kafka-tcp-service/common/constants"
	"io"
	"log"
	"net"
	"sync"
)

type TCPHandler struct {
	Marking string
	Conn    net.Conn
}

func NewTCPHandler(tcpHandler TCPHandler) *TCPHandler {
	return &tcpHandler
}

func StartTCPServer(wg *sync.WaitGroup) {
	listener, err := net.Listen("tcp", config.Configs.TcpPort)
	if err != nil {
		panic("Error lisetening: " + err.Error())
	}

	defer listener.Close()

	log.Println("TCP service listening on port", config.Configs.TcpPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error:", err)
			continue
		}

		// log.Printf("-----------------Connected: %v-----------------\n", conn.RemoteAddr())

		handler := NewTCPHandler(TCPHandler{Conn: conn})

		wg.Add(1)

		go func() {
			defer wg.Done()

			handler.handleClient()
		}()
	}
}

func (t *TCPHandler) handleClient() {

	buff := make([]byte, 1024*2)

	var dataLength int
	var err error

	defer t.closeConnection()

	for {
		dataLength, err = t.Conn.Read(buff)
		if err != nil {
			if err == io.EOF {
				// log.Printf("-----------------Disconnected: %v-----------------\n", t.Conn.RemoteAddr())
				break
			}
			// log.Println("error reading from connection:: ", err.Error())
			continue
		}

		dataBytes := buff[:dataLength]

		if len(dataBytes) < constants.MarkingLength {
			log.Printf("invalid data received Len: %d, data: %x\n", len(dataBytes), dataBytes)
			continue
		}

		marking := string(dataBytes[:8])

		t.Marking = marking

		connections.AddConnection(marking, t.Conn)

		messenger.AddProducerJob(messenger.ProducerJob{
			Topic:   &config.Configs.KafkaProducerTopic,
			Message: dataBytes,
		})
	}
}

func (t *TCPHandler) closeConnection() {

	if err := t.Conn.Close(); err != nil {
		log.Println("error closing connection: ", err.Error())
		return
	}

	connections.RemoveConnection(t.Marking)
}
