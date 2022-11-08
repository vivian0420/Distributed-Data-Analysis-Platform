package clientlib

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
)


func HandlePutFile(msgHandler *messages.MessageHandler, action string, fileName string, destFileName string, contentType string, sizeOfChunk string) {
	file, err := os.Open(fileName)
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
	chunkSize, err := strconv.Atoi(sizeOfChunk)
	if err != nil {
		log.Fatal("os.Args[6] cannot be convert into int")
	}
	var chunks []*messages.Chunk
	if contentType == "-text" {
	chunks = chopText(file, destFileName, int64(chunkSize))
	} else {
	chunks = chopBinary(file, destFileName, int64(chunkSize))
	}
	fileMessage := messages.File{Fullpath: destFileName, Checksum: checksum, Size: size, Action: action, Chunks: chunks}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_File{File: &fileMessage},
	}
	msgHandler.Send(wrap)
	handlePut(msgHandler, file, destFileName, chunkSize, contentType)
}

func handlePut(msgHandler *messages.MessageHandler, file *os.File, path string, chunkSize int, contentType string) {
	defer msgHandler.Close()
	wrapper, _ := msgHandler.Receive()
	if !wrapper.GetFile().GetApproved() {
		log.Println("File: " + path + " existed!!!!")
		return
	}
	chunks := wrapper.GetFile().GetChunks()
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
		toConnect := c.GetReplicanodename()[0]
		conn, err := net.Dial("tcp", toConnect)
		if err != nil {
			log.Fatalln("fail to connect to storage node: "+toConnect, err.Error())
			return
		}
		storageMsgHandler := messages.NewMessageHandler(conn)
		wrap := &messages.Wrapper{
			Msg: &messages.Wrapper_Chunk{Chunk: c},
		}
		storageMsgHandler.Send(wrap)
	}
}

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