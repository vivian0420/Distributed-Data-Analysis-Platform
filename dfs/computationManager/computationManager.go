package main

import (
	"dfs/messages"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
)

var wg sync.WaitGroup
var sem = make(chan int, 10)

func handleJobs(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
	for {
		wrapper, _ := msgHandler.Receive()
		plugin := wrapper.GetJob().GetPlugin()
		inputFile := wrapper.GetJob().GetInput()
		//outputFile := wrapper.GetJob().GetOutput()
		conn, err := net.Dial("tcp", os.Args[1]+":20100")
		if err != nil {
			log.Fatalln(err.Error())
			return
		}
		defer conn.Close()
		controllerMsgHandler := messages.NewMessageHandler(conn)
		fileMessage := messages.File{Fullpath: inputFile, Action: "get"}
		wrap := &messages.Wrapper{
			Msg: &messages.Wrapper_File{File: &fileMessage},
		}
		controllerMsgHandler.Send(wrap)
		controllerWrapper, _ := controllerMsgHandler.Receive()
		if !wrapper.GetFile().GetApproved() {
			log.Println("File does not existed")
			return
		}
		chunks := controllerWrapper.GetFile().GetChunks()
		for i := 0; i < len(chunks); i++ {
			sem <- 1
			wg.Add(1)
			go sendPlugin(chunks[i], plugin, inputFile, uint64(i))
		}
		wg.Wait()
	}
}

func sendPlugin(chunk *messages.Chunk, plugin []byte, inputFile string, chunk_num uint64) {
	defer wg.Done()
	var toConn net.Conn
	for i := 0; i < len(chunk.Replicanodename); i++ {
		conn, err := net.Dial("tcp", chunk.Replicanodename[i])
		if err == nil {
			toConn = conn
			defer conn.Close()
			defer toConn.Close()
			break
		}
		if i == len(chunk.Replicanodename)-1 && err != nil {
			log.Fatalln(err)
			return
		}
	}
	sendPluginHandler := messages.NewMessageHandler(toConn)
	chunkMessage := messages.Job{Plugin: plugin, Input: inputFile, ChunkNum: chunk_num, Action: "map"}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_Job{Job: &chunkMessage},
	}
	sendPluginHandler.Send(wrap)
	<-sem
}

func main() {
	computationManager, err := net.Listen("tcp", ":20110")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	fmt.Println("Successed")
	for {
		if conn, err := computationManager.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			go handleJobs(msgHandler)
		}
	}
}
