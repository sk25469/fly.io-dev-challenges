package main

import (
	"encoding/json"
	"maelstrom-unique-ids/pkg/server"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

var logger *log.Logger

func initLogger() {
	logger = log.New()
	logFile, _ := os.OpenFile("./maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger.SetOutput(logFile)
}

func main() {
	initLogger()

	n := maelstrom.NewNode()
	tests := []string{"echo", "generate"}
	n.Handle(tests[1], func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// Update the message type to return back.
		body["type"] = "generate_ok"
		body["id"] = server.GenerateUniqueId()

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
