package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	mandatory = false
	immediate = false
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	byteStream, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Unable to marshal val %s", val)
	}

	ch.PublishWithContext(context.Background(), exchange, key, mandatory, immediate, amqp.Publishing{
		ContentType: "application/json",
		Body:        byteStream,
	})

	return nil
}

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	mainCh, err := conn.Channel()
	if err != nil {
		fmt.Printf("Could not create channel: %v\n", err)
		return nil, amqp.Queue{}, err
	}
	defer mainCh.Close()

	isDurable := (queueType == Durable)
	isTransient := (queueType == Transient)
	clientQueue, err := mainCh.QueueDeclare(queueName, isDurable, isTransient, isTransient, false, nil)
	if err != nil {
		fmt.Printf("Could not create queue: %v\n", err)
		return nil, amqp.Queue{}, err
	}

	err = mainCh.QueueBind(queueName, key, exchange, isTransient, nil)
	if err != nil {
		fmt.Printf("Could not bind queue: %v\n", err)
		return nil, amqp.Queue{}, err
	}

	return mainCh, clientQueue, nil
}
