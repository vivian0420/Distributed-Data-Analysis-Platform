package main

import (
	"log"
	"strings"
	"bufio"
	"net"
	"os"
        "fmt"
	"dfs/messages"
)

func main() {
	host := os.Args[1]
        conn, err := net.Dial("tcp", host+":8080") 
        if err != nil {
                log.Fatalln(err.Error())
                return
        }
        defer conn.Close()
	msgHandler := messages.NewMessageHandler(conn)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		result := scanner.Scan() // Reads up to a \n newline character
                if result == false {
                        break
                }

                message := scanner.Text()
		if len(message) != 0 {
			if strings.HasPrefix(message, "upload") {
				filePath := strings.Split(message, " ")[1]
				fi, err := os.Stat(filePath)
				if err != nil {
    					log.Fatalln(err.Error())
                       			return
				}	
				fileSize := fi.Size()
				destinationObject := messages.Destination{Type: "upload", Filename: filePath, Size: fileSize};
				rWrapper := &messages.Wrapper{
                                    Msg: &messages.Wrapper_DestinationMessage{DestinationMessage: &destinationObject},
                                }
                                msgHandler.Send(rWrapper)
			}
		}
		

	}



}           
