package main

import (
	"log"
	"net"
	"os"
	"io"
	"dfs/messages"
	"crypto/md5"
        "math"
	"sync"
)
//wg sync.WaitGroup -----> https://stackoverflow.com/questions/18207772/how-to-wait-for-all-goroutines-to-finish-without-using-time-sleep
const ChunkSize = 32

func handlePut(msgHandler *messages.MessageHandler, file *os.File, path string) {
	defer msgHandler.Close()
	order := uint64(0)
	wrapper, _ := msgHandler.Receive()
        if !wrapper.GetFile().GetApproved() {
	    log.Println("File existed!!!!")
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
                thisChunk := wrapper.GetFile().GetChunks()[order]
                toConnect := thisChunk.GetReplicanodename()[0]
                conn, err := net.Dial("tcp", toConnect)
                if err != nil {
			log.Fatalln("fail to connect to storage node: " + toConnect, err.Error())
                        return
		}
                msgHandler = messages.NewMessageHandler(conn)
                chunk := messages.Chunk{Fullpath: path, Order: order, Checksum: checksum[:], Size: uint64(bytesread), Action: "put", Content: chunk[:bytesread], Replicanodename: wrapper.GetFile().GetChunks()[order].GetReplicanodename()}
                wrap := &messages.Wrapper {
			Msg: &messages.Wrapper_Chunk{Chunk: &chunk},
		}
                msgHandler.Send(wrap)
                order++
        }

}

func handlePutFile(msgHandler *messages.MessageHandler, fileMessage messages.File) {
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
	chunkAmount := math.Ceil(float64(size) / float64(ChunkSize))
     	fileMessage = messages.File{Fullpath: os.Args[4], Checksum: checksum, Size:size, Chunkamount: uint64(chunkAmount), Action: os.Args[2]}
     	wrap := &messages.Wrapper{
                Msg: &messages.Wrapper_File{File: &fileMessage},
       	}
     	msgHandler.Send(wrap)
     	handlePut(msgHandler, file, os.Args[4])
}

var wg sync.WaitGroup
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
        log.Println("GetSize", wrapper.GetFile().GetSize())
    	defer file.Close()
    	if err != nil {
        	log.Fatal(err)
    	}
	//chunkAmount := wrapper.GetFile().GetChunkamount()
	chunks := wrapper.GetFile().GetChunks()	
	//checkSum := wrapper.GetFile().GetChecksum()
	for i := 0; i < len(chunks); i++ {
		wg.Add(1)
		go handleChunkDownload(chunks[i])
	}
	wg.Wait()
	//todo: checksum
	

}

func handleChunkDownload(chunk *messages.Chunk){
	defer wg.Done()
	conn, err := net.Dial("tcp", chunk.Replicanodename[0])
	if err != nil {
                log.Fatalln(err.Error())
                return
        }
        defer conn.Close()
	downloadHandler := messages.NewMessageHandler(conn)
	chunkMessage := messages.Chunk{Fullpath: chunk.GetFullpath(), Order: chunk.GetOrder(), Action: "get"}
	wrap := &messages.Wrapper{
                Msg: &messages.Wrapper_Chunk{Chunk: &chunkMessage},
        }
        downloadHandler.Send(wrap)
	wrapper, _ := downloadHandler.Receive()
	order := wrapper.GetChunk().GetOrder()
	content := wrapper.GetChunk().GetContent()
	skip := int64(ChunkSize * order)
        file, err := os.OpenFile(os.Args[4], os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("OpenFile failed:", err)
                return
	}
	file.Seek(skip, 0)
        file.Write(content)
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
	/*
	chunks := wrapper.GetFile().GetChunks()
	for i := 0; i < len(chunks); i++ {
                wg.Add(1)
                go handleChunkDelete(chunks[i])
        }
        wg.Wait()
	*/
}

/*
func handleChunkDelete(chunk *messages.Chunk) {
	defer wg.Done()
        conn, err := net.Dial("tcp", chunk.Replicanodename[0])
        if err != nil {
                log.Fatalln(err.Error())
                return
        }
        defer conn.Close()
	deleteHandler := messages.NewMessageHandler(conn)
	chunkMessage := messages.Chunk{Fullpath: chunk.GetFullpath(), Order: chunk.GetOrder(), Action: "delete"}
	wrap := &messages.Wrapper{
                Msg: &messages.Wrapper_Chunk{Chunk: &chunkMessage},
        }
        deleteHandler.Send(wrap)	
}
*/

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
		handlePutFile(msgHandler, fileMessage)
		return
	} else if os.Args[2] == "get" {
		handleGetFile(msgHandler)
		return
	} else if os.Args[2] == "delete" {
		handleDeleteFile(msgHandler)
   		return
	} else if os.Args[2] == "ls" {
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
