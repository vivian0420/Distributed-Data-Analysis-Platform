package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"dfs/messages"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
)

//wg sync.WaitGroup -----> https://stackoverflow.com/questions/18207772/how-to-wait-for-all-goroutines-to-finish-without-using-time-sleep

func chopText(file *os.File, fullpath string, chunkSize int64) []*messages.Chunk {
	file.Seek(0, 0)
	var chunks []*messages.Chunk
	reader := bufio.NewReader(file)
	var buffer bytes.Buffer
	start := int64(0)

	for {
		bytesread, err := reader.ReadBytes('\n')
		buffer.Write(bytesread)
		if len(buffer.Bytes()) >= int(chunkSize) || err == io.EOF {
			chunkcontent := make([]byte, len(buffer.Bytes()))
			copy(chunkcontent, buffer.Bytes())
			checksum := md5.Sum(chunkcontent)
			chunk := messages.Chunk{
				Fullpath: fullpath,
				Checksum: checksum[:],
				Start:    start,
				Size:     uint64(len(chunkcontent)),
				Action:   "put",
			}

			start += int64(len(chunkcontent))
			chunks = append(chunks, &chunk)
			buffer.Reset()
		}
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
				panic(err)
			}
			break
		}
	}
	return chunks
}

func chopBinary(file *os.File, fullpath string, chunkSize int64) []*messages.Chunk {
	file.Seek(0, 0)
	var chunks []*messages.Chunk
	start := int64(0)
	for {
		chunkcontent := make([]byte, chunkSize)
		bytesread, err := file.Read(chunkcontent)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}
		checksum := md5.Sum(chunkcontent[:bytesread])

		chunk := messages.Chunk{
			Fullpath: fullpath,
			Checksum: checksum[:],
			Size:     uint64(bytesread),
			Action:   "put",
			Start:    start,
		}
		start += chunkSize
		chunks = append(chunks, &chunk)
	}
	return chunks
}

func handlePut(msgHandler *messages.MessageHandler, file *os.File, path string, chunkSize int, contentType string, chunks []*messages.Chunk) {
	defer msgHandler.Close()
	order := uint64(0)
	wrapper, _ := msgHandler.Receive()
	if !wrapper.GetFile().GetApproved() {
		log.Println("File existed!!!!")
		return
	}
	file.Seek(0, 0)
	for _, c := range chunks {
		file.Seek(c.Start, 0)
		content := make([]byte, c.Size)
		bytesread, err := file.Read(content)
		if err != nil {
			log.Fatal("Cannot read content for upload: ", err)
			return
		}
		c.Content = content[:bytesread]
		toConnect := wrapper.GetFile().GetChunks()[order].GetReplicanodename()[0]
		conn, err := net.Dial("tcp", toConnect)
		if err != nil {
			log.Fatalln("fail to connect to storage node: "+toConnect, err.Error())
			return
		}
		msgHandler = messages.NewMessageHandler(conn)
		c.Replicanodename = wrapper.GetFile().GetChunks()[order].GetReplicanodename()
		c.Order = order
		wrap := &messages.Wrapper{
			Msg: &messages.Wrapper_Chunk{Chunk: c},
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
	chunkSize, err := strconv.Atoi(os.Args[6])
	if err != nil {
		log.Fatal("os.Args[6] cannot be convert into int")
	}
	var chunks []*messages.Chunk
	contentType := os.Args[5]
	if contentType == "-text" {
		chunks = chopText(file, os.Args[4], int64(chunkSize))
	} else {
		chunks = chopBinary(file, os.Args[4], int64(chunkSize))
	}
	fileMessage := messages.File{Fullpath: os.Args[4], Checksum: checksum, Size: size, Action: os.Args[2], Chunks: chunks}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_File{File: &fileMessage},
	}
	msgHandler.Send(wrap)
	handlePut(msgHandler, file, os.Args[4], chunkSize, contentType, chunks)
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
