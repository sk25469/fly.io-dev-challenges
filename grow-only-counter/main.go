package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	maxWriteRetries    = 10
	writeRetryInterval = 1000 * time.Millisecond
	maxReadRetries     = 5
	readRetryInterval  = 1000 * time.Millisecond
	key                = "global-counter"
)

var (
	lastSeen = 0
	mut      sync.RWMutex
)

func getLastSeen() int {
	mut.RLock()
	defer mut.RUnlock()
	return lastSeen
}

func setLastSeen(value int) {
	mut.Lock()
	defer mut.Unlock()
	lastSeen = value
}

func storeInKV(kv *maelstrom.KV, intVal float64) error {
	for retries := 0; retries < maxWriteRetries; retries++ {
		value, err := kv.ReadInt(context.Background(), key)
		if err != nil {
			if strings.Contains(err.Error(), "key does not exist") {
				value = 0
			} else {
				return err
			}
		}

		newValue := value + int(intVal)
		setLastSeen(newValue)

		log.Printf("new value after update: %v ---------------- %v", newValue, getLastSeen())

		err = kv.Write(context.Background(), key, newValue)
		if err == nil {
			return nil
		} else {
			log.Printf("retrying.... %v -------------- %v", retries, err)
			time.Sleep(writeRetryInterval)
		}
	}
	return errors.New("max write retries exceeded")
}

func syncData(kv *maelstrom.KV, intVal float64) {
	if err := storeInKV(kv, intVal); err != nil {
		log.Printf("syncData failed: %v", err)
	}
}

func retryGetValue(kv *maelstrom.KV) (int, error) {
	for retries := 0; retries < maxReadRetries; retries++ {
		value, err := kv.ReadInt(context.Background(), key)
		if err != nil {
			if strings.Contains(err.Error(), "key does not exist") {
				err = kv.Write(context.Background(), key, getLastSeen())
				if err != nil {
					log.Printf("retrying.... %v -------------- %v", retries, err)
					time.Sleep(readRetryInterval)
					continue
				}
				value = getLastSeen()
			} else {
				return 0, err
			}
		}

		setLastSeen(value)
		log.Printf("last seen value after GET - %v ----------- %v", getLastSeen(), value)
		return getLastSeen(), nil
	}
	return getLastSeen(), errors.New("max read retries exceeded")
}

func getValueFromKV(kv *maelstrom.KV) int {
	value, err := retryGetValue(kv)
	if err != nil {
		log.Printf("getValueFromKV failed: %v", err)
	}
	return value
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	node.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val, ok := body["delta"].(float64)
		if !ok {
			return errors.New("error in converting to int")
		}

		go syncData(kv, val)

		body["type"] = "add_ok"
		delete(body, "delta")

		return node.Reply(msg, body)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["value"] = getValueFromKV(kv)

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
