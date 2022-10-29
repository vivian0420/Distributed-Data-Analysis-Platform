package main

import (
	"dfs/messages"
	"log"
	"net"
	"os"
)

func main() {
	host := os.Args[4]
	conn, err := net.Dial("tcp", host+":20110")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer conn.Close()
	bytes, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	msgHandler := messages.NewMessageHandler(conn)
	jobMessage := messages.Job{Plugin: bytes, Input: os.Args[2], Output: os.Args[3]}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_Job{Job: &jobMessage},
	}
	msgHandler.Send(wrap)
}
