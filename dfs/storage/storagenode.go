package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"dfs/clientlib"
	"dfs/messages"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"plugin"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

var numOfRequests uint64 = 0
var pluginMap = make(map[string]*plugin.Plugin)
var l sync.Mutex

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
		case *messages.Wrapper_Job:
			if msg.Job.GetAction() == "map" {
				chunkNum := wrapper.GetJob().GetChunkNum()
				chunkPath := filepath.Join(os.Args[1], wrapper.GetJob().GetInput(), fmt.Sprintf("chunk-%d", chunkNum))
				plu := wrapper.GetJob().GetPlugin()
				outputFilePath := wrapper.GetJob().GetOutput()
				reducers := wrapper.GetJob().GetReducername()
				pluginName := wrapper.GetJob().GetPluginName()
				id := wrapper.GetJob().GetJobId()
				pluginPath := filepath.Join(os.Args[1], pluginName)
				chunk, err := os.Open(chunkPath)
				if err != nil {
					panic(err)
				}
				defer chunk.Close()
				l.Lock()
				if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
					f, err := os.Create(pluginPath)
					if err != nil {
						panic(err)
					}
					defer f.Close()
					_, err1 := f.Write(plu)
					if err1 != nil {
						panic(err1)
					}
				}
				if _, ok := pluginMap[pluginPath]; !ok {
					p, err := plugin.Open(pluginPath)
					if err != nil {
						log.Println("Panic happend on chunk: ", chunkNum)
						panic(err)
					}
					pluginMap[pluginPath] = p
				}
				l.Unlock()
				lineNum := 0
				var mapResult []*map[string]uint32
				scanner := bufio.NewScanner(chunk)
				//map
				m, err := pluginMap[pluginPath].Lookup("Map")
				if err != nil {

					panic(err)
				}
				for scanner.Scan() {
					mapResult = append(mapResult, (m.(func(int, string) []*map[string]uint32)(lineNum, scanner.Text()))...)
					lineNum += 1
				}
				if err := scanner.Err(); err != nil {
					log.Fatal(err)
				}
				//shuffle
				s, err := pluginMap[pluginPath].Lookup("Shuffle")
				if err != nil {
					panic(err)
				}
				shuffleResult := s.(func([]*map[string]uint32, uint32) map[int]([]*map[string]uint32))(mapResult, uint32(len(reducers)))

				// send reduce job to reducers
				for i, reducer := range reducers {
					conn, err := net.Dial("tcp", reducer)
					if err != nil {
						log.Fatalln(err)
						return
					}
					var reducejob []*messages.MapPair
					for _, eachMap := range shuffleResult[i] {
						for k, v := range *eachMap {
							mapPair := messages.MapPair{
								Key:   k,
								Value: v,
							}
							reducejob = append(reducejob, &mapPair)
						}
					}
					jobHandler := messages.NewMessageHandler(conn)
					mappairs := messages.MapPairs{Reducejob: reducejob}
					jobMessage := messages.Job{MapPairs: &mappairs, ReducerIndex: uint32(i), Action: "mapdone", Output: outputFilePath, ChunkNum: chunkNum, JobId: id}
					wrap := &messages.Wrapper{
						Msg: &messages.Wrapper_Job{Job: &jobMessage},
					}
					log.Println("Send mapdone message to reducer: ", chunkNum)
					jobHandler.Send(wrap)
					jobHandler.Receive()
				}
				// send completing map job message to computation manager:
				mapCompleteMessage := messages.Job{Action: "map Completed"}
				mapCompleteWrap := &messages.Wrapper{
					Msg: &messages.Wrapper_Job{Job: &mapCompleteMessage},
				}
				clientHandler.Send(mapCompleteWrap)
				log.Println("Send map Completed message to computation manager: ", chunkNum)

			} else if msg.Job.GetAction() == "mapdone" {
				id := wrapper.GetJob().GetJobId()
				mapPairs := wrapper.GetJob().GetMapPairs()
				chunkNum := wrapper.GetJob().GetChunkNum()
				log.Println("Received mapdone data from chun: ", chunkNum)
				jobDirPath := filepath.Join(os.Args[1], id)
				jobPath := filepath.Join(jobDirPath, fmt.Sprint(chunkNum))
				os.MkdirAll(jobDirPath, os.ModePerm)
				f, err := os.Create(jobPath)
				if err != nil {
					panic(err)
				}
				defer f.Close()
				out, _ := proto.Marshal(mapPairs)
				_, err1 := f.Write(out)
				if err1 != nil {
					panic(err1)
				}
				saveCompletedMessage := messages.Job{Action: "saved"}
				saveCompletedWrap := &messages.Wrapper{
					Msg: &messages.Wrapper_Job{Job: &saveCompletedMessage},
				}
				clientHandler.Send(saveCompletedWrap)

			} else if msg.Job.GetAction() == "reduce" {
				outputFilePath := wrapper.GetJob().GetOutput()
				reducerIndex := wrapper.GetJob().GetReducerIndex()
				id := wrapper.GetJob().GetJobId()
				jobDirPath := filepath.Join(os.Args[1], id)
				totalChunks := wrapper.GetJob().GetTotalChunk()
				mapResult := messages.MapPairs{}
				for i := 0; i < int(totalChunks); i++ {
					jobPath := filepath.Join(jobDirPath, fmt.Sprint(i))
					in, err := os.ReadFile(jobPath)
					if err != nil {
						log.Fatalln("Error reading map pair file:", err)
					}
					mappairs := &messages.MapPairs{}
					if err := proto.Unmarshal(in, mappairs); err != nil {
						log.Fatalln("Failed to parse Files:", err)
					}
					mapResult.Reducejob = append(mapResult.Reducejob, mappairs.Reducejob...)
				}
				listOfMap := []*map[string]uint32{}
				for _, mappair := range mapResult.Reducejob {
					eachMap := make(map[string]uint32)
					eachMap[mappair.Key] = mappair.Value
					listOfMap = append(listOfMap, &eachMap)
				}
				pluginName := wrapper.GetJob().GetPluginName()
				pluginPath := os.Args[1] + "/" + pluginName
				plu := wrapper.GetJob().GetPlugin()
				if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
					f, err := os.Create(pluginPath)
					if err != nil {
						panic(err)
					}
					defer f.Close()
					_, err1 := f.Write(plu)
					if err1 != nil {
						panic(err1)
					}
				}
				p, err := plugin.Open(pluginPath)
				if err != nil {
					panic(err)
				}
				//sort
				s, err := p.Lookup("Sort")
				if err != nil {
					panic(err)
				}
				sortResult := s.(func([]*map[string]uint32) []*map[string][]uint32)(listOfMap)
				r, err := p.Lookup("Reduce")
				if err != nil {
					panic(err)
				}
				//Reduce
				reduceResult := r.(func([]*map[string][]uint32) []*map[string]uint32)(sortResult)
				// write "reduceResult" to txt file
				f, err := os.Create(os.Args[1] + "/reduceResult.txt")
				if err != nil {
					log.Fatal(err)
				}
				defer f.Close()
				for _, m := range reduceResult {
					for k, v := range *m {
						_, err := f.WriteString(k + ": " + fmt.Sprint(v) + "\n")
						if err != nil {
							log.Fatal(err)
						}
					}
				}
				//send completing reduce job message to computation manager:
				reduceCompleteMessage := messages.Job{Action: "reduce Completed"}
				reduceCompleteWrap := &messages.Wrapper{
					Msg: &messages.Wrapper_Job{Job: &reduceCompleteMessage},
				}
				clientHandler.Send(reduceCompleteWrap)
				//upload reduce result file dfs
				conToController, err := net.Dial("tcp", os.Args[2]+":20100")
				if err != nil {
					log.Fatalln(err.Error())
					return
				}
				defer conToController.Close()
				putMsgHandler := messages.NewMessageHandler(conToController)
				outputPath := filepath.Join(outputFilePath, fmt.Sprintf("result-%d", reducerIndex))
				clientlib.HandlePutFile(putMsgHandler, "put", os.Args[1]+"/reduceResult.txt", outputPath, "-text", "102400")
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
	chunk, err := os.ReadFile(chunkPath)
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
	if !success {
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
