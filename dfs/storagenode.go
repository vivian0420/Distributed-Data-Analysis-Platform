package main

import (
	"os"
	"log"
	"net"
)

func main() {
	cHost := os.Args[1]  //controller host name
        oHost := os.Args[2]  //this host name
        //connect to controller
        conn, err := net.Dial("tcp", cHost+":8080") 
        if err != nil {
                log.Fatalln(err.Error())
                return
        }
        defer conn.Close()
        msgHandler := messages.NewMessageHandler(conn)
        //send registration message to controller
        storagepath := "/bigdata/jzhang230/storage"
	reMessage := messages.Registration{StoragePath: storagePath, Host: oHost}
	wrap := &messages.Wrapper{
             Msg: &messages.Wrapper_RegistrationMessage{RegistrationMessage: &reMessage},
        }
        msgHandler.Send(wrap)
	
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
	
	//send heartbeat to controller periodically
	go heartBeat()


}

func heartBeat() {



}

func handleClient(msgHandler *messages.MessageHandler) {




}
