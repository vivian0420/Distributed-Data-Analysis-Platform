package main

import (
	"bytes"
	"crypto/md5"
	"dfs/messages"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"dfs/clientlib"
)

//wg sync.WaitGroup -----> https://stackoverflow.com/questions/18207772/how-to-wait-for-all-goroutines-to-finish-without-using-time-sleep
var wg sync.WaitGroup
var sem = make(chan int, 10)

func handleGetFile(msgHandler *messages.MessageHandler) {
	fileMessage := messages.File{Fullpath: os.Args[3], Action: os.Args[2]}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_File{File: &fileMessage},
	}
	msgHandler.Send(wrap)
	wrapper, _ := msgHandler.Receive()
	if !wrapper.GetFile().GetApproved() {
		log.Println("File does not existed")
		return
	}
	file, err := os.Create(os.Args[4])
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	chunks := wrapper.GetFile().GetChunks()
	chunkSize := wrapper.GetFile().GetChunksize()
	providedCheckSum := wrapper.GetFile().GetChecksum()

	for i := 0; i < len(chunks); i++ {
		sem <- 1
		wg.Add(1)
		chunks[i].Order = uint64(i)
		go handleChunkDownload(chunks[i], chunkSize)
	}
	wg.Wait()
	//validate checksum
	wholeFile, err := os.Open(os.Args[4])
	if err != nil {
		log.Fatal(err)
	}
	defer wholeFile.Close()
	h := md5.New()
	if _, err := io.Copy(h, wholeFile); err != nil {
		log.Fatal(err)
	}
	fileChecksum := h.Sum(nil)
	res := bytes.Compare(providedCheckSum, fileChecksum)
	if res == 0 {
		log.Printf("Download file %s successfully", os.Args[3])
	} else {
		log.Printf("Failed to download file %s.", os.Args[3])
	}

}

func handleChunkDownload(chunk *messages.Chunk, chunkSize uint64) {
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
	downloadHandler := messages.NewMessageHandler(toConn)
	chunkMessage := messages.Chunk{Fullpath: chunk.GetFullpath(), Order: chunk.GetOrder(), Action: "get"}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_Chunk{Chunk: &chunkMessage},
	}
	downloadHandler.Send(wrap)
	wrapper, _ := downloadHandler.Receive()
	content := wrapper.GetChunk().GetContent()
	skip := chunk.GetStart()
	file, err := os.OpenFile(os.Args[4], os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("OpenFile failed:", err)
		return
	}
	defer file.Close()
	file.Seek(skip, 0)
	file.Write(content)
	<-sem
}

func handleDeleteFile(msgHandler *messages.MessageHandler) {
	fileMessage := messages.File{Fullpath: os.Args[3], Action: os.Args[2]}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_File{File: &fileMessage},
	}
	msgHandler.Send(wrap)
	wrapper, _ := msgHandler.Receive()
	if !wrapper.GetFile().GetApproved() {
		log.Println("File does not existed")
		return
	}
}

func handleListFile(msgHandler *messages.MessageHandler) {
	fileMessage := messages.File{Fullpath: os.Args[3], Action: os.Args[2]}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_File{File: &fileMessage},
	}
	msgHandler.Send(wrap)
	wrapper, _ := msgHandler.Receive()
	if !wrapper.GetFiles().GetApproved() {
		log.Println("File does not existed")
		return
	} else {
		files := wrapper.GetFiles().GetFiles()
		uniqueMap := make(map[string]bool)
		for _, file := range files {
			_, ok := uniqueMap[file.GetFullpath()]
			if !ok {
				fmt.Println(file.GetFullpath())
				uniqueMap[file.GetFullpath()] = true
			}

		}
	}
}

func handleListNode(msgHandler *messages.MessageHandler) {
	fileMessage := messages.File{Action: os.Args[2]}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_File{File: &fileMessage},
	}
	msgHandler.Send(wrap)
	wrapper, _ := msgHandler.Receive()
	hosts := wrapper.GetHosts().GetHosts()
	for i, host := range hosts {
		fmt.Printf("----------Host%d----------\n", i)
		fmt.Println("Host name: ", host.GetName())
		fmt.Println("Free space: ", host.GetFreespace())
		fmt.Println("Requests: ", host.GetRequests())
	}
}

func main() {
	host := os.Args[1]
	conn, err := net.Dial("tcp", host+":20100")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer conn.Close()
	msgHandler := messages.NewMessageHandler(conn)
	if os.Args[2] == "put" {
		clientlib.HandlePutFile(msgHandler, os.Args[2], os.Args[3], os.Args[4], os.Args[5], os.Args[6])
	} else if os.Args[2] == "get" {
		handleGetFile(msgHandler)
	} else if os.Args[2] == "delete" {
		handleDeleteFile(msgHandler)
	} else if os.Args[2] == "ls" {
		handleListFile(msgHandler)
	} else if os.Args[2] == "listnode" {
		handleListNode(msgHandler)
	} else {
		log.Println("Invalid action: ", os.Args[2])
	}

}
