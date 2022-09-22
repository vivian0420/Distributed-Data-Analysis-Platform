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
	"io/ioutil"
	"bytes"
        "path/filepath"
)

var numOfRequests uint64 = 0
func main() {
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

        go handleHeartBeat(msgHandler, thisHostName + os.Args[3])   
    
        //establish server socket for listenning from clients
	listener, err := net.Listen("tcp", os.Args[3])
        if err != nil {
                log.Fatalln(err.Error())
                return
        }
        for {
                if conn, err := listener.Accept(); err == nil {
                        clientHandler := messages.NewMessageHandler(conn)
                        go handleClient(clientHandler,  msgHandler, thisHostName + os.Args[3])
                }
        }
	

}

func handleHeartBeat(msgHandler *messages.MessageHandler, thisHostName string) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-  ticker.C:
		var stat unix.Statfs_t
                wd, _ := os.Getwd()
		unix.Statfs(wd, &stat)
		hbMessage := messages.HeartBeat{Name: thisHostName, Requests: numOfRequests, FreeSpace: stat.Bavail * uint64(stat.Bsize)}
		wrap := &messages.Wrapper{
             		Msg: &messages.Wrapper_Heartbeat{Heartbeat: &hbMessage},
        	}
        	msgHandler.Send(wrap) 
		fmt.Println("send heartbeat to controller")
		}
	}
}

func handleClient(clientHandler *messages.MessageHandler, msgHandler *messages.MessageHandler, thisHostName string) {
	defer clientHandler.Close()
         for {
                wrapper, _ := clientHandler.Receive()
                switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_Chunk:
			action := msg.Chunk.GetAction()
			if action == "put" {
				path := filepath.Join(os.Args[1], msg.Chunk.GetFullpath())
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
                                log.Println("order", order)
                                log.Println("string(order)", string(order))
				chunkPath := filepath.Join(path, fmt.Sprintf("chunk-%d", order))
                                log.Println("chunkPath", chunkPath)
				err = os.WriteFile(chunkPath, content, 0644)
		        	if err != nil {
                			log.Fatal(err)
        			}
				file, err := os.Open(chunkPath)
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
				if success == false {
					e := os.Remove(chunkPath)
    					if e != nil {
        					log.Fatal(e)
    					}
					statusMessage := messages.Status{Success: success, Order: order}
					wrap := &messages.Wrapper{
						Msg: &messages.Wrapper_Status{Status: &statusMessage},
					}
					log.Println("send error message to client")
					clientHandler.Send(wrap)
				} else {
                        	        replicates := msg.Chunk.GetReplicanodename()

                        	        index := -1
                        	        for i, v := range replicates {
						if (v == thisHostName) {
							index = i
						}
					}
                        	        if index == len(replicates) - 1 {
						log.Println("Last node, no need to replicate")
						return
					}
					toReplicate := replicates[index + 1]
					conn, err := net.Dial("tcp", toReplicate)
					if err != nil {
                        	                 log.Fatalln(err.Error())
                        	                 return
                        	        }
                        	        replicateHandler := messages.NewMessageHandler(conn)
					replicateHandler.Send(wrapper)
				}
			} else if action == "get" {
				order := msg.Chunk.GetOrder()
				chunkPath := filepath.Join(os.Args[1], msg.Chunk.GetFullpath(), fmt.Sprintf("chunk-%d", order))
                                log.Println("chunkPath:", chunkPath)
				chunk, err := ioutil.ReadFile(chunkPath)
				if err != nil {
      					log.Printf("Could not open the chunk due to this %s error \n", err)
   				}
				chunkMessage := messages.Chunk{Order: order, Content: chunk}
				wrap := &messages.Wrapper{
                                                Msg: &messages.Wrapper_Chunk{Chunk: &chunkMessage},
                                }
                                clientHandler.Send(wrap)
			}

			
                case nil:
                        //log.Println("Received an empty message, terminating client ")
                        return
                default:
                        log.Printf("Unexpected message type: %T", msg)
			
		}
	}
}


