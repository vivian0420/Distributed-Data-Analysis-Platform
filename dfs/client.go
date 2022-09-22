package main

import (
	"log"
	"net"
	"os"
	"io"
	"dfs/messages"
	"crypto/md5"
        "math"
)

const ChunkSize = 32

func handlePut(msgHandler *messages.MessageHandler, file *os.File, path string) {
	defer msgHandler.Close()
	order := uint64(0)
	wrapper, _ := msgHandler.Receive()
        if !wrapper.GetFile().GetApproved() {
            log.Println("file already exists but I still continue")
            return
        }
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
                log.Println("GetChunks():", wrapper.GetFile().GetChunks())
                thisChunk := wrapper.GetFile().GetChunks()[order]
                toConnect := thisChunk.GetReplicanodename()[0]
                conn, err := net.Dial("tcp", toConnect)
                if err != nil {
			log.Fatalln("fail to connect to storage node: " + toConnect, err.Error())
                        return
		}
                msgHandler = messages.NewMessageHandler(conn)
                chunk := messages.Chunk{Fullpath: path, Order: order, Checksum: checksum[:], Size: uint64(bytesread), Content: chunk[:bytesread], Replicanodename: wrapper.GetFile().GetChunks()[order].GetReplicanodename()}
                wrap := &messages.Wrapper {
			Msg: &messages.Wrapper_Chunk{Chunk: &chunk},
		}
                msgHandler.Send(wrap)
                order++
                log.Println("order: ", order)
        }

//	switch msg := wrapper.Msg.(type) {
//	case *messages.Wrapper_Approbation:
//		approved := msg.Approbation.GetApproved()
//		if approved {
//			chunk := make([]byte, ChunkSize)
//                        file.Seek(0, 0)
//			for {
//				bytesread, err := file.Read(chunk)
//				if err != nil {
//					if err != io.EOF {
//						log.Println(err)
//					}
//					break
//			  	}
//				checksum := md5.Sum(chunk[:bytesread])
//				chunkMessage := messages.Chunk{Fullpath: path, Order: order, Checksum: checksum[:], Size: uint64(bytesread)}
//				wrap := &messages.Wrapper{
//                			Msg: &messages.Wrapper_Chunk{Chunk: &chunkMessage},
//                		}
//                		msgHandler.Send(wrap)		
//				replywrapper, _ := msgHandler.Receive()
//        			switch msg := replywrapper.Msg.(type) {
//        			case *messages.Wrapper_Host:
//                			host := msg.Host.GetName()
//                			conn, err := net.Dial("tcp", host)
//                			if err != nil {
//                        			log.Fatalln(err.Error())
//                        			return
//                			}
//                			defer conn.Close()
//                			uploadHandler := messages.NewMessageHandler(conn)
//					chunkUploadMessage := messages.Chunk{Fullpath: path, Order: order, Checksum: checksum[:], Size: uint64(bytesread), Content: chunk[:bytesread]}
//					chunkWrap := &messages.Wrapper{
//						Msg: &messages.Wrapper_Chunk{Chunk: &chunkUploadMessage},
//					}
//					uploadHandler.Send(chunkWrap)
//					order++
//					/*
//					uploadWrapper, err := uploadHandler.Receive()
//					if err != nil {
//						uploadMsg := uploadWrapper.Msg.(type)
//						order := uploadMsg.Status.GetOrder()
//						log.Printf("Upload file %s failed on chunk %d", file, order)
//						return
//					}
//					*/
//				default:
//                  			log.Printf("Unexpected message type while ask storage nodes for uploading: %T", msg)	
//				}
//			}
//		}
//	default:
//        	log.Printf("Unexpected message type while ask approbation for uploading: %T", msg)
//	}
}

func handlePutFile(msgHandler *messages.MessageHandler, args2 string, args3 string, args4 string, fileMessage messages.File) {
	file, err := os.Open(args3)
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
	chunkAmount := math.Ceil(float64(size) / float64(ChunkSize))
        log.Println("chunk amount", chunkAmount)
     	fileMessage = messages.File{Fullpath: args4, Checksum: checksum, Size:size, Chunkamount: uint64(chunkAmount), Action: args2}
     	wrap := &messages.Wrapper{
                Msg: &messages.Wrapper_File{File: &fileMessage},
       	}
     	msgHandler.Send(wrap)
     	handlePut(msgHandler, file, args4)
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
		handlePutFile(msgHandler, os.Args[2], os.Args[3], os.Args[4], fileMessage)
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
