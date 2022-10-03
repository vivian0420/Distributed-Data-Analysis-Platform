package main

import (
	"bytes"
	"crypto/md5"
	"dfs/messages"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

var numOfRequests uint64 = 0

func main() {
	host := os.Args[2] //controller hostname
	//connect to controller
	conn, err := net.Dial("tcp", host+":20100")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer conn.Close()
	msgHandler := messages.NewMessageHandler(conn)
	thisHostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	//go handleController(msgHandler)
	go handleHeartBeat(msgHandler, thisHostName+os.Args[3])

	//establish server socket for listenning from clients
	listener, err := net.Listen("tcp", os.Args[3])
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	for {
		if conn, err := listener.Accept(); err == nil {
			clientHandler := messages.NewMessageHandler(conn)
			go handleClient(clientHandler, thisHostName+os.Args[3])
		}
	}

}

func handleHeartBeat(msgHandler *messages.MessageHandler, thisHostName string) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			var stat unix.Statfs_t
			wd, _ := os.Getwd()
			unix.Statfs(wd, &stat)
			hbMessage := messages.HeartBeat{Name: thisHostName, Requests: numOfRequests, FreeSpace: stat.Bavail * uint64(stat.Bsize)}
			wrap := &messages.Wrapper{
				Msg: &messages.Wrapper_Heartbeat{Heartbeat: &hbMessage},
			}
			msgHandler.Send(wrap)
			fmt.Println("send heartbeat to controller")
		}
	}
}

func handleClient(clientHandler *messages.MessageHandler, thisHostName string) {
	defer clientHandler.Close()
	for {
		wrapper, _ := clientHandler.Receive()
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_Chunk:
			action := msg.Chunk.GetAction()
			if action == "put" {
				//Create a new file for chunk. Path is the file's path + chunk's order
				//if checksum doesn't match, notify client
				shouldReturn := handleActionPut(msg, clientHandler, thisHostName, wrapper)
				if shouldReturn {
					return
				}
			} else if action == "get" {
				handleActionGet(msg, clientHandler)
			}
		case *messages.Wrapper_File:
			if msg.File.GetAction() == "delete" {
				numOfRequests++
				path := filepath.Join(os.Args[1], wrapper.GetFile().GetFullpath())
				e := os.RemoveAll(path)
				if e != nil {
					log.Fatal(e)
				}
			}
		case nil:
			return
		default:
			log.Printf("Unexpected message type: %T", msg)

		}
	}
}

func handleActionGet(msg *messages.Wrapper_Chunk, clientHandler *messages.MessageHandler) {
	numOfRequests++
	order := msg.Chunk.GetOrder()
	chunkPath := filepath.Join(os.Args[1], msg.Chunk.GetFullpath(), fmt.Sprintf("chunk-%d", order))
	chunk, err := ioutil.ReadFile(chunkPath)
	if err != nil {
		log.Printf("Could not open the chunk due to this %s error \n", err)
	}
	chunkMessage := messages.Chunk{Order: order, Content: chunk}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_Chunk{Chunk: &chunkMessage},
	}
	clientHandler.Send(wrap)
}

func handleActionPut(msg *messages.Wrapper_Chunk, clientHandler *messages.MessageHandler, thisHostName string, wrapper *messages.Wrapper) bool {
	numOfRequests++
	path := filepath.Join(os.Args[1], msg.Chunk.GetFullpath())
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			log.Fatal(err)
		}
	}
	order := msg.Chunk.GetOrder()
	content := msg.Chunk.GetContent()
	checksumProvided := msg.Chunk.GetChecksum()

	chunkPath := filepath.Join(path, fmt.Sprintf("chunk-%d", order))
	err = os.WriteFile(chunkPath, content, 0644)
	if err != nil {
		log.Fatal(err)
	}
	file, err := os.Open(chunkPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	h := md5.New()
	if _, err := io.Copy(h, file); err != nil {
		log.Fatal(err)
	}
	chunkChecksum := h.Sum(nil)
	success := true

	res := bytes.Compare(chunkChecksum, checksumProvided)
	if res != 0 {
		success = false
	}
	if success == false {
		e := os.Remove(chunkPath)
		if e != nil {
			log.Fatal(e)
		}
		statusMessage := messages.Status{Success: success, Order: order}
		wrap := &messages.Wrapper{
			Msg: &messages.Wrapper_Status{Status: &statusMessage},
		}
		log.Println("send error message to client")
		clientHandler.Send(wrap)
	} else {
		replicates := msg.Chunk.GetReplicanodename()

		index := -1
		for i, v := range replicates {
			if v == thisHostName {
				index = i
			}
		}
		if index == len(replicates)-1 {
			return true
		}
		toReplicate := replicates[index+1]
		conn, err := net.Dial("tcp", toReplicate)
		if err != nil {
			log.Fatalln(err.Error())
			return true
		}
		replicateHandler := messages.NewMessageHandler(conn)
		replicateHandler.Send(wrapper)
	}
	return false
}
