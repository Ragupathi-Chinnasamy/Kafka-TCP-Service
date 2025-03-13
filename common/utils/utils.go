package utils

import (
	"fmt"
	"net"
	"time"
)

func GenerateConnectionID(conn net.Conn) string {
	return fmt.Sprintf("%s-%d", conn.RemoteAddr().String(), time.Now().UnixNano())
}

func GetISTTime() string {
	locationIST, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		locationIST = time.UTC
	}

	currentTimeIST := time.Now().In(locationIST).Format("02-01-2006 15:04:05")
	return currentTimeIST
}
