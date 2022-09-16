package main

import (
	"os"
	"log"
	"net"
	"golang.org/x/sys/unix"
	"dfs/messages"
	"time"
	"fmt"
)

var numOfRequests uint64 = 0
func main() {
	//path := os.Args[1]  //provide a storage directory path
        host := os.Args[2]  //controller hostname
        //connect to controller
        conn, err := net.Dial("tcp", host+":8080") 
        if err != nil {
                log.Fatalln(err.Error())
                return
        }
        defer conn.Close()
        msgHandler := messages.NewMessageHandler(conn)
	thisHostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

        go handleHeartBeat(msgHandler, thisHostName)   
    
        //establish server socket for listenning from clients
	listener, err := net.Listen("tcp", ":6666")
        if err != nil {
                log.Fatalln(err.Error())
                return
        }
        for {
                if conn, err := listener.Accept(); err == nil {
                        clientHandler := messages.NewMessageHandler(conn)
                        go handleClient(clientHandler)
                }
        }
	

}

func handleHeartBeat(msgHandler *messages.MessageHandler, hostname string) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-  ticker.C:
		var stat unix.Statfs_t
                wd, _ := os.Getwd()
		unix.Statfs(wd, &stat)
           
		hbMessage := messages.HeartBeat{Name: hostname, Requests: numOfRequests, FreeSpace: stat.Bavail * uint64(stat.Bsize)}
		wrap := &messages.Wrapper{
             		Msg: &messages.Wrapper_Heartbeat{Heartbeat: &hbMessage},
        	}
        	msgHandler.Send(wrap) 
		fmt.Println("send heartbeat to controller")
		}
	}
}

func handleClient(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
         for {
                wrapper, _ := msgHandler.Receive()

                switch msg := wrapper.Msg.(type) {
                case nil:
                        log.Println("Received an empty message, terminating client ")
                        return
                default:
                        log.Printf("Unexpected message type: %T", msg)
			
		}
	}
}


