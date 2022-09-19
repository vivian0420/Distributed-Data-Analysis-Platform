package main

import (
	"log"
	"net"
	"os"
	"io"
	"dfs/messages"
	"crypto/md5"
)

const ChunkSize = 32

func handlePut(msgHandler *messages.MessageHandler, file *os.File, path string) {
	defer msgHandler.Close()
	order := uint64(1)
	wrapper, _ := msgHandler.Receive()
	switch msg := wrapper.Msg.(type) {
	case *messages.Wrapper_Approbation:
		approved := msg.Approbation.GetApproved()
		if approved {
			chunk := make([]byte, ChunkSize)
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
				chunkMessage := messages.Chunk{Fullpath: path, Order: order, Checksum: checksum[:], Size: uint64(bytesread)}
				wrap := &messages.Wrapper{
                			Msg: &messages.Wrapper_Chunk{Chunk: &chunkMessage},
                		}
                		msgHandler.Send(wrap)		
				replywrapper, _ := msgHandler.Receive()
        			switch msg := replywrapper.Msg.(type) {
        			case *messages.Wrapper_Host:
                			host := msg.Host.GetName()
					log.Println(host)
                			conn, err := net.Dial("tcp", host)
                			if err != nil {
                        			log.Fatalln(err.Error())
                        			return
                			}
                			defer conn.Close()
                			uploadHandler := messages.NewMessageHandler(conn)
					chunkUploadMessage := messages.Chunk{Fullpath: path, Order: order, Checksum: checksum[:], Size: uint64(bytesread), Content: chunk[:bytesread]}
					chunkWrap := &messages.Wrapper{
						Msg: &messages.Wrapper_Chunk{Chunk: &chunkUploadMessage},
					}
					uploadHandler.Send(chunkWrap)
				
				default:
                  			log.Printf("Unexpected message type while ask storage nodes for uploading: %T", msg)	
				}
			}
		}
	default:
        	log.Printf("Unexpected message type while ask approbation for uploading: %T", msg)
	}
}

func main() {
	host := os.Args[1]
        conn, err := net.Dial("tcp", host+":8080") 
        if err != nil {
                log.Fatalln(err.Error())
                return
        }
        defer conn.Close()
	msgHandler := messages.NewMessageHandler(conn)
	fileMessage := messages.File{}
	if os.Args[2] == "put" {
                
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
		fileMessage = messages.File{Fullpath: os.Args[4], Checksum: checksum, Size:size, Action: os.Args[2]}
		wrap := &messages.Wrapper{
                Msg: &messages.Wrapper_File{File: &fileMessage},
        	}
        	msgHandler.Send(wrap)
        	handlePut(msgHandler, file, os.Args[4])
		return
	} else if os.Args[2] == "get" ||  os.Args[2] == "delete" || os.Args[2] == "ls" {
		fileMessage = messages.File{Fullpath: os.Args[3], Action: os.Args[2]}
	} else if os.Args[2] == "listnode" {
		fileMessage = messages.File{Action: os.Args[2]}
	} else {
		log.Println("Invalid action: ", os.Args[2])
		return
	}
	wrap := &messages.Wrapper{
                Msg: &messages.Wrapper_File{File: &fileMessage},
        } 
        msgHandler.Send(wrap)
	//handleController(msgHandler)

}
