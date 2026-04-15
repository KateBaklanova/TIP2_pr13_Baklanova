package main

import (
	"encoding/json"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

type TaskEvent struct {
	Event  string `json:"event"`
	TaskID string `json:"task_id"`
	TS     string `json:"ts"`
}

func main() {
	rabbitURL := os.Getenv("RABBIT_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}
	queueName := os.Getenv("QUEUE_NAME")
	if queueName == "" {
		queueName = "task_events"
	}

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("rabbit connect error: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel error: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatalf("queue declare error: %v", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatalf("qos error: %v", err)
	}

	msgs, err := ch.Consume(
		queueName,
		"",
		false, // auto-ack = false
		false, false, false, nil,
	)
	if err != nil {
		log.Fatalf("consume error: %v", err)
	}

	log.Println("worker started, waiting for messages...")

	for d := range msgs {
		var ev TaskEvent
		if err := json.Unmarshal(d.Body, &ev); err != nil {
			log.Printf("bad message: %v", err)
			d.Nack(false, false)
			continue
		}
		log.Printf("received event=%s task_id=%s ts=%s", ev.Event, ev.TaskID, ev.TS)
		if err := d.Ack(false); err != nil {
			log.Printf("ack error: %v", err)
		}
	}
}
