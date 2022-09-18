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
			approved := true
			in, err := ioutil.ReadFile(DFSController)
			if err != nil {
			        log.Fatalln("Error reading file:", err)
			}
			files := &messages.Files{}
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
		case nil:
			continue
                default:
                        log.Printf("Unexpected message type: %T", msg)
		}
	}
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
