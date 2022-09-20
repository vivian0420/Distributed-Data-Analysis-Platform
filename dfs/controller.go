package main

import(
	"dfs/messages"
	"fmt"
        "log"
        "net"
	"time"
	"io/ioutil"
	"google.golang.org/protobuf/proto"

)

type activednode struct {
	freeSpace uint64
	requests uint64
	timeStamp int64
}


var  activedNodes = make(map[string] activednode)
var totalSpace = uint64(0)
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
			totalSpace += msg.Heartbeat.GetFreeSpace()
		case *messages.Wrapper_File:
			action := msg.File.GetAction()
			if action == "put" {
				handleClientPut(msgHandler, msg)
			} else if action == "get" {
							
			}
		case *messages.Wrapper_Chunk:
			for _, file := range files.GetFiles() {
                                if file.GetFullpath() == msg.Chunk.GetFullpath() {
					file.Chunks = append(file.GetChunks(), msg.Chunk)
					break
                                }
                        }
			out, _ := proto.Marshal(files)
                        if err := ioutil.WriteFile(DFSController, out, 0644); err != nil {
                                log.Fatalln("Failed to write Files:", err)
                        }
			host := getGreatestSpace(msg.Chunk.GetHosts())
			hostMessage := messages.Host{Name: host}
			wrap := &messages.Wrapper{
                                Msg: &messages.Wrapper_Host{Host: &hostMessage},
                        }
                        msgHandler.Send(wrap)
		case *messages.Wrapper_Status:
			size := 0
			for _, file := range files.GetFiles() {
                                if file.GetFullpath() == msg.Status.GetFullpath() {
					for _, chunk := range file.GetChunks() {
						if chunk.GetOrder() == msg.Status.GetOrder() {
							host := &messages.Host{Name: msg.Status.GetName()}
							chunk.Hosts = append(chunk.GetHosts(), host)
							size = len(chunk.Hosts)
							if size < 3 {
								replicaNode := getGreatestSpace(chunk.Hosts)
								log.Println("The node to be replicated is: ", replicaNode)
							        hostMessage := messages.Host{Name: replicaNode}
                        					wrap := &messages.Wrapper{
                                					Msg: &messages.Wrapper_Host{Host: &hostMessage},
                        					}
                        					msgHandler.Send(wrap)
							}
						}
					}
				}
			}
			out, _ := proto.Marshal(files)
                        if err := ioutil.WriteFile(DFSController, out, 0644); err != nil {
                                log.Fatalln("Failed to write Files:", err)
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

func getGreatestSpace(hosts []*messages.Host) string {
	max := uint64(0)
	host := ""
	for key, value := range activedNodes {
		if value.freeSpace > max && !contains(hosts, key) {
			host = key
		}
	}
	log.Println("return host: ", host)
	return host
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
	for _, file := range files.GetFiles() {
	        if file.GetFullpath() == msg.File.GetFullpath() {
	                approved = false
	        }
	}
	size := msg.File.GetSize()
	if size > totalSpace / 3 {
	        approved = false
	}
	if approved {
	        files.Files = append(files.GetFiles(), msg.File)
	        out, _ := proto.Marshal(files)
	        if err := ioutil.WriteFile(DFSController, out, 0644); err != nil {
	                log.Fatalln("Failed to write Files:", err)
	        }
	}
	fmt.Println(approved)
	approbationMessage := messages.Approbation{Approved: approved}
	wrap := &messages.Wrapper{
	        Msg: &messages.Wrapper_Approbation{Approbation: &approbationMessage},
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
