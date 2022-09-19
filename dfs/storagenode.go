package main

import (
	"os"
	"log"
	"net"
	"golang.org/x/sys/unix"
	"dfs/messages"
	"time"
	"fmt"
	"crypto/md5"
	"io"
	"bytes"
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

        go handleHeartBeat(msgHandler, thisHostName, os.Args[3])   
    
        //establish server socket for listenning from clients
	listener, err := net.Listen("tcp", os.Args[3])
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

func handleHeartBeat(msgHandler *messages.MessageHandler, hostname string, port string) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-  ticker.C:
		var stat unix.Statfs_t
                wd, _ := os.Getwd()
		unix.Statfs(wd, &stat)
		fmt.Println(hostname+port)
		fmt.Println(numOfRequests)
		fmt.Println(stat.Bavail * uint64(stat.Bsize))           
		hbMessage := messages.HeartBeat{Name: hostname+port, Requests: numOfRequests, FreeSpace: stat.Bavail * uint64(stat.Bsize)}
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
		
		case *messages.Wrapper_Chunk:
			path := "/bigdata/jzhang230/storage" + msg.Chunk.GetFullpath() 
			_, err := os.Stat(path)
			if os.IsNotExist(err) {
				if err := os.MkdirAll(path, os.ModePerm); err != nil {
        				log.Fatal(err)
    				}		
			}
			order := msg.Chunk.GetOrder()
			content := msg.Chunk.GetContent()
			checksumProvided := msg.Chunk.GetChecksum()
			//Create a new file for chunk. Path is the file's path + chunk's order
			err = os.WriteFile(path+"-"+string(order), content, 0644)
		        if err != nil {
                		log.Fatal(err)
        		}
			file, err := os.Open(os.Args[3])
                	if err != nil {
                		log.Fatal(err)
                	}
                	defer file.Close()
			h := md5.New()
                	if _, err := io.Copy(h, file); err != nil {
                        	log.Fatal(err)
                	}
                	chunkChecksum := h.Sum(nil)
			success := true
			//if checksum doesn't match, notify client
			res := bytes.Compare(chunkChecksum, checksumProvided)
			if res != 0 {
				success = false
			}
			statusMessage := messages.Status{Success: success}
			wrap := &messages.Wrapper{
				Msg: &messages.Wrapper_Status{Status: &statusMessage},
			}
			msgHandler.Send(wrap) 
                case nil:
                        log.Println("Received an empty message, terminating client ")
                        return
                default:
                        log.Printf("Unexpected message type: %T", msg)
			
		}
	}
}


