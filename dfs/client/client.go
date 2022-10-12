package main

import (
	"bytes"
	"crypto/md5"
	"dfs/messages"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"sync"
)

//wg sync.WaitGroup -----> https://stackoverflow.com/questions/18207772/how-to-wait-for-all-goroutines-to-finish-without-using-time-sleep

func handlePut(msgHandler *messages.MessageHandler, file *os.File, path string, intchunksize int) {
	defer msgHandler.Close()
	order := uint64(0)
	wrapper, _ := msgHandler.Receive()
	if !wrapper.GetFile().GetApproved() {
		log.Println("File existed!!!!")
		return
	}
	chunk := make([]byte, intchunksize)
	file.Seek(0, 0)
	for {
		bytesread, err := file.Read(chunk)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}
		checksum := md5.Sum(chunk[:bytesread])
		thisChunk := wrapper.GetFile().GetChunks()[order]
		toConnect := thisChunk.GetReplicanodename()[0]
		conn, err := net.Dial("tcp", toConnect)
		if err != nil {
			log.Fatalln("fail to connect to storage node: "+toConnect, err.Error())
			return
		}
		msgHandler = messages.NewMessageHandler(conn)
		chunk := messages.Chunk{Fullpath: path, Order: order, Checksum: checksum[:], Size: uint64(bytesread), Action: "put", Content: chunk[:bytesread], Replicanodename: wrapper.GetFile().GetChunks()[order].GetReplicanodename()}
		wrap := &messages.Wrapper{
			Msg: &messages.Wrapper_Chunk{Chunk: &chunk},
		}
		msgHandler.Send(wrap)
		order++
	}

}

func handlePutFile(msgHandler *messages.MessageHandler) {
	file, err := os.Open(os.Args[3])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	fi, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	size := uint64(fi.Size())
	h := md5.New()
	if _, err := io.Copy(h, file); err != nil {
		log.Fatal(err)
	}
	checksum := h.Sum(nil)
	intchunksize, _ := strconv.Atoi(os.Args[5])
	chunkAmount := math.Ceil(float64(size) / float64(intchunksize))
	fileMessage := messages.File{Fullpath: os.Args[4], Checksum: checksum, Size: size, Chunksize: uint64(intchunksize), Chunkamount: uint64(chunkAmount), Action: os.Args[2]}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_File{File: &fileMessage},
	}
	msgHandler.Send(wrap)
	handlePut(msgHandler, file, os.Args[4], intchunksize)
}

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
	order := wrapper.GetChunk().GetOrder()
	content := wrapper.GetChunk().GetContent()
	skip := int64(chunkSize * order)
	file, err := os.OpenFile(os.Args[4], os.O_WRONLY, 0644)
	defer file.Close()
	if err != nil {
		log.Fatal("OpenFile failed:", err)
		return
	}
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
		handlePutFile(msgHandler)
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