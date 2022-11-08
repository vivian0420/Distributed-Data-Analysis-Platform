package main

import (
	"dfs/messages"
	"log"
	"net"
	"os"
	"path"
	
)

func main() {
	host := os.Args[4]
	conn, err := net.Dial("tcp", host+":20110")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer conn.Close()
	bytes, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	msgHandler := messages.NewMessageHandler(conn)
	//reducerNum, err := strconv.Atoi(os.Args[5])
	if err != nil {
		log.Fatal("os.Args[5] cannot be convert into int")
	}
	jobMessage := messages.Job{Plugin: bytes, PluginName: path.Base(os.Args[1]), Input: os.Args[2], Output: os.Args[3], Action: "submit job"}
	wrap := &messages.Wrapper{
		Msg: &messages.Wrapper_Job{Job: &jobMessage},
	}
	msgHandler.Send(wrap)
	completedMap := 0
	completedReduce := 0
	for {
		wrapper, _ := msgHandler.Receive()
		if wrapper.GetFeedback().GetType() == "map" {
			completedMap++
			log.Println(completedMap)
			completedPercentage := float64(completedMap) / float64(wrapper.GetFeedback().GetTotalMap())
			log.Printf("map completed: %0.2f%%\n", completedPercentage * 100)

		} else if wrapper.GetFeedback().GetType() == "reduce" {
			completedReduce++
			totalReducer := wrapper.GetFeedback().GetTotalReduce()
			log.Println("totalReducer: ", totalReducer)
			completedPercentage := float64(completedReduce) / float64(totalReducer)
			log.Printf("reduce completed: %0.2f%%\n", completedPercentage * 100)
			if completedPercentage == 1 {
				log.Println("Done MapReduce!")
				return
			}
		}
        
	}
}
