package main

import (
	"dfs/messages"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sort"

	"github.com/google/uuid"
)

var wg sync.WaitGroup

var sem = make(chan int, 10)

func handleJobs(msgHandler *messages.MessageHandler) {
	log.Println("received job!!!!")
	defer msgHandler.Close()
	for {
		wrapper, _ := msgHandler.Receive()
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_Job:
			action := msg.Job.GetAction()
			var reducernum uint32
			var chunks []*messages.Chunk
			if action == "submit job" {
				id := uuid.New()
				plugin := wrapper.GetJob().GetPlugin()
				inputFile := wrapper.GetJob().GetInput()
				reducernum = wrapper.GetJob().GetReducerNum()
				outputFile := wrapper.GetJob().GetOutput()
				name := wrapper.GetJob().GetPluginName()
				conn, err := net.Dial("tcp", os.Args[1]+":20100")
				if err != nil {
					log.Fatalln(err.Error())
					return
				}
				defer conn.Close()
				//send request to controller to ask chunks' info
				controllerMsgHandler := messages.NewMessageHandler(conn)
				fileMessage := messages.File{Fullpath: inputFile, Action: "get"}
				wrap := &messages.Wrapper{
					Msg: &messages.Wrapper_File{File: &fileMessage},
				}
				controllerMsgHandler.Send(wrap)
				controllerWrapper, _ := controllerMsgHandler.Receive()
				if !controllerWrapper.GetFile().GetApproved() {
					log.Println("File does not existed")
					return
				}
				chunks = controllerWrapper.GetFile().GetChunks()
				//sort replicanodes
				mapperList := []string{}
				mapperSort := make(map[string]int)
				for _, chunk := range chunks {
					mapperSort[chunk.Replicanodename[1]] = mapperSort[chunk.Replicanodename[1]] + 1
				}
				log.Println("mapperSort: ", mapperSort)
				for key := range mapperSort {
					mapperList = append(mapperList, key)
				}
				sort.SliceStable(mapperList, func(i, j int) bool{
					return mapperSort[mapperList[i]] > mapperSort[mapperList[j]]
				})

				//send request to controller to ask activenodes' list
				fileMessageList := messages.File{Action: "listnode"}
				listwrap := &messages.Wrapper{
					Msg: &messages.Wrapper_File{File: &fileMessageList},
				}
				controllerMsgHandler.Send(listwrap)
				listwrapper, _ := controllerMsgHandler.Receive()
				hosts := listwrapper.GetHosts().GetHosts()
				//build reducers list
				var reducers []string
				for i := 0; i < int(reducernum); i++ {
					for _, host := range hosts {
						if host.GetName() == mapperList[i] {
							reducers = append(reducers, mapperList[i])
						}
					}
				}
				log.Println("reducers: ", reducers)
				//send map jobs to nodes
                for i := 0; i < len(chunks); i++ {
					sem <- 1
					wg.Add(1)
					go sendPlugin(chunks[i], plugin, inputFile, outputFile, uint64(i), reducers, name, msgHandler, chunks, reducernum, id)
					mapFeedbackMessage := messages.Feedback{TotalMap: uint64(len(chunks)), Type: "map"}
					mapFeedbackWrap := &messages.Wrapper{
						Msg: &messages.Wrapper_Feedback{Feedback: &mapFeedbackMessage},
					}
					msgHandler.Send(mapFeedbackWrap)
				}
				wg.Wait()
				//Map done! Send reduce message to reducers:
				for i, reducer := range reducers {
					conn, err := net.Dial("tcp", reducer)
					if err != nil {
						log.Fatalln(err)
						return
					}
					defer conn.Close()
					reducerMsgHandler := messages.NewMessageHandler(conn)
					reduceessage := messages.Job{Plugin: plugin, PluginName: name, Input: inputFile, Output: outputFile, TotalChunk: uint64(len(chunks)), Action: "reduce", ReducerIndex: uint32(i), JobId: id.String()}
					wrap := &messages.Wrapper{
						Msg: &messages.Wrapper_Job{Job: &reduceessage},
					}
					reducerMsgHandler.Send(wrap)
					reducerMsgHandler.Receive()
					reduceFeedbackMessage := messages.Feedback{TotalReduce: uint64(reducernum), Type: "reduce"}
					reduceFeedbackWrap := &messages.Wrapper{
						Msg: &messages.Wrapper_Feedback{Feedback: &reduceFeedbackMessage},
					}
					msgHandler.Send(reduceFeedbackWrap)
				}

			}

		}
	}
}

func sendPlugin(chunk *messages.Chunk, plugin []byte, inputFile string, outputFile string, chunk_num uint64, reducers []string, name string, msgHandler *messages.MessageHandler,
	chunks []*messages.Chunk, reducernum uint32, id uuid.UUID) {
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
	chunkMessage := messages.Job{Plugin: plugin, PluginName: name, JobId: id.String(), Input: inputFile, Output: outputFile, ChunkNum: chunk_num, Action: "map", Reducername: reducers, TotalChunk: uint64(len(chunks))}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_Job{Job: &chunkMessage},
	}
	sendPluginHandler.Send(wrap)
	sendPluginHandler.Receive()
	log.Println("Received map completed message from storage node. Chunk: ", chunk_num)
	<-sem
}

func main() {
	computationManager, err := net.Listen("tcp", ":20120")
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
