package main

import(
	"dfs/messages"
	"fmt"
        "log"
        "net"
	"time"
	"math/rand"
	"io/ioutil"
	"google.golang.org/protobuf/proto"

)

type activednode struct {
	freeSpace uint64
	requests uint64
	timeStamp int64
}


var activedNodes = make(map[string] activednode)
const DFSController = "dfs-controller.bin"
var files = &messages.Files{}

func handleClient(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
	 for {
                wrapper, _ := msgHandler.Receive()
                switch msg := wrapper.Msg.(type) {
                case *messages.Wrapper_Heartbeat:
			nodeName := msg.Heartbeat.GetName()
			node := activednode{msg.Heartbeat.GetFreeSpace(),  msg.Heartbeat.GetRequests(),  time.Now().Unix()}
			activedNodes[nodeName] = node
		case *messages.Wrapper_File:
			action := msg.File.GetAction()
			if action == "put" {
				handleClientPut(msgHandler, msg)
			} else if action == "get" {
				approved := false
				for _, f := range files.GetFiles() {
                			if f.GetFullpath() == msg.File.GetFullpath() {
						approved  = true
                                                f.Approved = true
                        			wrap := &messages.Wrapper {
                                			Msg: &messages.Wrapper_File{File: f},
                        			}
                        			msgHandler.Send(wrap)
						break
                			}
        			}
				if approved == false {
					file := messages.File{Approved: false}
                        		wrap := &messages.Wrapper {
                                		Msg: &messages.Wrapper_File{File: &file},
                        		}
                        		msgHandler.Send(wrap)
                        		return
				}
				
			}
		case nil:
			continue
                default:
                        log.Printf("Unexpected message type: %T", msg)
		}
	}
}

func contains(hosts []*messages.Host, name string) bool {
	for _, v := range hosts {
		if v.GetName() == name {
			return true
		}
	}

	return false
}

func getRandomNodes() (map[string]bool) {
        nodes := make(map[string]bool)
        var min int
        if len(activedNodes) < 3 {
            min = len(activedNodes)
        } else {
            min = 3
        }

        keys := make([]string, 0, len(activedNodes))
	for k := range activedNodes {
		keys = append(keys, k)
	}

	rand.Seed(time.Now().UnixNano())
        for len(nodes) < min {
		i := rand.Intn(min)
                node := keys[i]
                if _, ok := nodes[node]; !ok {
			nodes[node] = true
		}
        }

        return nodes
}

func checkLiveness() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select{
		case <- ticker.C:
			for name, info := range activedNodes{
				if time.Now().Unix() - info.timeStamp > 15 {
					log.Println("Lost connection with node: ", name)
					delete (activedNodes, name)
				}
			}
		}
	}
}

func handleClientPut(msgHandler *messages.MessageHandler, msg *messages.Wrapper_File) {
	approved := true
	in, err := ioutil.ReadFile(DFSController)
	if err != nil {
	        log.Fatalln("Error reading file:", err)
	}
	if err := proto.Unmarshal(in, files); err != nil {
	        log.Fatalln("Failed to parse Files:", err)
	}
	for _, f := range files.GetFiles() {
	        if f.GetFullpath() == msg.File.GetFullpath() {
	                file := messages.File{Approved: false}
                        wrap := &messages.Wrapper {
				Msg: &messages.Wrapper_File{File: &file},
                        }
                        msgHandler.Send(wrap)
                        return
	        }
	}
	log.Println("approved: ",approved)
	chunkAmount := int(msg.File.GetChunkamount())
        file := messages.File{Fullpath: msg.File.GetFullpath(), Approved: true, Chunkamount: msg.File.GetChunkamount()}
	for i := 0; i < chunkAmount; i++ {
                chunk := messages.Chunk{Fullpath: file.GetFullpath(), Order: uint64(i)}
                for node, _ := range getRandomNodes() {
			chunk.Replicanodename = append(chunk.Replicanodename, node)
                }
                file.Chunks = append(file.Chunks, &chunk)
        }
	files.Files = append(files.Files, &file)
	out, _ := proto.Marshal(files)
	if err := ioutil.WriteFile(DFSController, out, 0644); err != nil {
	        log.Fatalln("Failed to write Files:", err)
	}
        wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_File{File: &file},
        }
	msgHandler.Send(wrap)
}

func main() {
	// Read the existing Files.
	_, err := ioutil.ReadFile(DFSController)
	if err != nil {
		files := &messages.Files{}
		// Write the new Files back to disk.
		out, _ := proto.Marshal(files)
		if err := ioutil.WriteFile(DFSController, out, 0644); err != nil {
		        log.Fatalln("Failed to write Files:", err)
		}
	}

	controller, err := net.Listen("tcp", ":8080")
        if err != nil {
                log.Fatalln(err.Error())
                return
        }
        fmt.Println("Successed")
        go checkLiveness()
	for {
                if conn, err := controller.Accept(); err == nil {
                        msgHandler := messages.NewMessageHandler(conn)
                        go handleClient(msgHandler)
                }
        }

	




}           
