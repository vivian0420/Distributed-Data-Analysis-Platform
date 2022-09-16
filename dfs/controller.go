package main

import(
	"dfs/messages"
	"fmt"
        "log"
        "net"
)

func handleClient(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
	 for {
                wrapper, _ := msgHandler.Receive()
                switch msg := wrapper.Msg.(type) {
                case *messages.Wrapper_Heartbeat:
			continue;
		case nil:
                        log.Println("Received an empty message, terminating client ")
                        return
                default:
                        log.Printf("Unexpected message type: %T", msg)
		}
	}


}

func main() {
	controller, err := net.Listen("tcp", ":8080")
        if err != nil {
                log.Fatalln(err.Error())
                return
        }
        fmt.Println("Successed")
	 for {
                if conn, err := controller.Accept(); err == nil {
                        msgHandler := messages.NewMessageHandler(conn)
                        go handleClient(msgHandler)
                }
        }

	




}           
