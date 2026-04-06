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
