package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maelstrom-broadcast/pkg/server"
	"os"
	"slices"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

// n req
// m nodes
// n * m * log(x)

type Topology struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

var graph map[string][]string = make(map[string][]string)
var nodes []string

var list []float64 = make([]float64, 0)

var logger *log.Logger
var mut sync.RWMutex

func search(numbers []float64, value float64) bool {
	index := sort.SearchFloat64s(numbers, value)
	return index < len(numbers) && numbers[index] == value
}

func bfs(from string, n *maelstrom.Node, writeBody map[string]any, readBody map[string]any) {
	visited := make(map[string]bool)
	queue := make([]string, 0)
	queue = append(queue, from)
	visited[from] = true

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		for _, neighbor := range graph[node] {
			if !visited[neighbor] {
				visited[neighbor] = true
				go replicateData(neighbor, n, writeBody)
				go sendRead(neighbor, n, readBody, writeBody)
				queue = append(queue, neighbor)
			}
		}
	}
}

func matchData(currData []float64, nextData []float64, neighbor string, n *maelstrom.Node, writeBody map[string]any) {
	// check for all the data in currnode
	newWriteBody := copyMap(writeBody)
	for _, val := range currData {
		if !search(nextData, val) {
			newWriteBody["message"] = val
			n.Send(neighbor, newWriteBody)
		}
	}

	for _, val := range nextData {
		if !search(currData, val) {
			list = append(list, val)
		}
	}
}

func sendRead(to string, n *maelstrom.Node, readBody map[string]any, writeBody map[string]any) error {
	msg, err := n.SyncRPC(context.Background(), to, readBody)
	if err != nil {
		fmt.Printf("error sending read req to %v: [%v]", to, err)
		return err
	} else {
		var res map[string]any
		if err := json.Unmarshal(msg.Body, &res); err != nil {
			return err
		}

		var nextData []float64
		if messages, ok := res["messages"].([]interface{}); ok {
			for _, v := range messages {
				if floatVal, ok := v.(float64); ok {
					nextData = append(nextData, floatVal)
				}
			}
		}

		go matchData(list, nextData, to, n, writeBody)
	}
	return nil
}

func replicateData(to string, n *maelstrom.Node, body map[string]any) error {
	replicated := false
	for !replicated {
		msg, err := n.SyncRPC(context.Background(), to, body)
		if err != nil {
			replicated = false
		} else {
			var res map[string]any
			if err := json.Unmarshal(msg.Body, &res); err != nil {
				return err
			}
			if res["type"] == "broadcast_ok" {
				replicated = true
				return nil
			}
		}
	}
	return nil
}

func copyMap(m map[string]any) map[string]any {
	newMap := make(map[string]any)
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}

func initLogger() {
	logger = log.New()
	logFile, _ := os.OpenFile("./maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger.SetOutput(logFile)
}

func main() {
	initLogger()

	n := maelstrom.NewNode()
	tests := []string{"echo", "generate", "broadcast", "read", "topology"}

	n.Handle(tests[0], func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "echo_ok"
		return n.Reply(msg, body)
	})

	n.Handle(tests[1], func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "generate_ok"
		body["id"] = server.GenerateUniqueId()
		return n.Reply(msg, body)
	})

	n.Handle(tests[2], func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		value := body["message"]
		floatValue, ok := value.(float64)
		if !ok {
			logger.Println("Value is not of type float64")
			return fmt.Errorf("value is not of type float64")
		}

		bodyToSend := copyMap(body)
		readBody := copyMap(body)
		// delete(readBody, "message")
		// readBody["type"] = "read"

		mut.Lock()
		if !search(list, floatValue) {
			list = append(list, floatValue)
			slices.Sort(list)
			go bfs(n.ID(), n, body, readBody)
		}
		mut.Unlock()
		delete(bodyToSend, "message")
		bodyToSend["type"] = "broadcast_ok"

		return n.Reply(msg, bodyToSend)
	})

	n.Handle(tests[3], func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// bodyToSend := copyMap(body)

		// fmt.Printf("body: %v", bodyToSend)
		// for key, value := range bodyToSend {
		// 	fmt.Printf("key: %v ----------- value: %v\n", key, value)
		// }

		body["type"] = "read_ok"
		mut.RLock()
		body["messages"] = list
		mut.RUnlock()

		return n.Reply(msg, body)
	})

	n.Handle(tests[4], func(msg maelstrom.Message) error {
		if len(nodes) == 0 {
			nodes = n.NodeIDs()
		}

		var topo Topology
		err := json.Unmarshal(msg.Body, &topo)
		if err != nil {
			log.Fatalf("Error unmarshalling JSON: %v", err)
		}

		for node, neighbors := range topo.Topology {
			graph[node] = neighbors
		}

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "topology_ok"
		delete(body, "topology")

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
