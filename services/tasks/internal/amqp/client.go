package amqp

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func MustConnect(url string) *amqp.Connection {
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("rabbit connect error: %v", err)
	}
	return conn
}
