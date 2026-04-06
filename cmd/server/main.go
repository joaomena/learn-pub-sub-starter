package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	brokerUrl = "amqp://guest:guest@localhost:5672/"
)

func main() {
	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial(brokerUrl)
	if err != nil {
		fmt.Printf("Could not connect to RabbitMQ broker: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Println("Server started up successfully!")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	defer fmt.Println("Received exit signal, bye!")

	mainCh, err := conn.Channel()
	if err != nil {
		fmt.Printf("Could not create channel: %v\n", err)
		return
	}
	defer mainCh.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pubsub.PublishJSON(mainCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
			IsPaused: true,
		})
	}
}
