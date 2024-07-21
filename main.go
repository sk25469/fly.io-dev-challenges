package main

import (
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

func bfs(from string, n *maelstrom.Node, body map[string]any) {
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
				replicateData(neighbor, n, body)
				queue = append(queue, neighbor)
			}
		}
	}
}

func replicateData(to string, n *maelstrom.Node, body map[string]any) {
	n.Send(to, body)
}

func initLogger() {
	logger = log.New()
	logFile, err := os.OpenFile("./maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	logger.SetOutput(logFile)
	logger.SetLevel(log.DebugLevel)
	logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
}

func main() {
	initLogger()

	n := maelstrom.NewNode()
	tests := []string{"echo", "generate", "broadcast", "read", "topology"}

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

		if !search(list, floatValue) {
			mut.Lock()
			list = append(list, floatValue)
			slices.Sort(list)
			bfs(n.ID(), n, body)
			mut.Unlock()
		}
		delete(body, "message")
		body["type"] = "broadcast_ok"

		return n.Reply(msg, body)

	})

	n.Handle(tests[3], func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

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
