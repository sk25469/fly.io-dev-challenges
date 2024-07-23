package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
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

const (
	maxRetries    = 100
	retryInterval = 1 * time.Second
)

var (
	graph map[string][]string = make(map[string][]string)
	// nodes             []string
	list              []float64 = make([]float64, 0)
	replicationStatus           = make(map[string]map[float64]bool)
	logger            *log.Logger
	mut               sync.RWMutex
)

func search(numbers []float64, value float64) bool {
	index := sort.SearchFloat64s(numbers, value)
	return index < len(numbers) && numbers[index] == value
}

func bfs(from string, n *maelstrom.Node, val float64, writeBody map[string]any) {
	for _, neighbor := range graph[from] {
		if !isReplicated(neighbor, val) {
			go matchData(neighbor, n, val, writeBody)
		}
	}
}

func markReplicated(neighbor string, data float64) {
	mut.Lock()
	defer mut.Unlock()
	replicationStatus[neighbor][data] = true
}

func isReplicated(neighbor string, data float64) bool {
	mut.RLock()
	defer mut.RUnlock()
	if _, exists := replicationStatus[neighbor]; !exists {
		replicationStatus[neighbor] = make(map[float64]bool)
	}
	return replicationStatus[neighbor][data]
}

func sendAndConfirmReplication(neighbor string, n *maelstrom.Node, writeBody map[string]any) error {
	ackCh := make(chan error, 1)

	n.RPC(neighbor, writeBody, func(msg maelstrom.Message) error {
		var res map[string]any
		if err := json.Unmarshal(msg.Body, &res); err != nil {
			ackCh <- err
			return err
		}
		if res["type"] == "broadcast_ok" {
			ackCh <- nil
		} else {
			ackCh <- errors.New("unexpected response")
		}
		return nil
	})

	select {
	case err := <-ackCh:
		return err
	case <-time.After(retryInterval):
		return errors.New("timeout")
	}
}

func matchData(neighbor string, n *maelstrom.Node, val float64, writeBody map[string]any) {
	newWriteBody := copyMap(writeBody)
	newWriteBody["message"] = val
	retries := 0

	for retries < maxRetries {
		err := sendAndConfirmReplication(neighbor, n, newWriteBody)
		if err == nil {
			markReplicated(neighbor, val)
			return
		}
		retries++
		time.Sleep(retryInterval)
	}
}

func insertData(floatValue float64, n *maelstrom.Node, body map[string]any) {
	if !search(list, floatValue) {
		list = append(list, floatValue)
		slices.Sort(list)
		go bfs(n.ID(), n, floatValue, body)
	}
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

func generateUniqueId() string {
	id := uuid.New()
	return id.String()
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
		body["id"] = generateUniqueId()
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

		go insertData(floatValue, n, body)
		delete(bodyToSend, "message")
		bodyToSend["type"] = "broadcast_ok"

		return n.Reply(msg, bodyToSend)
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
