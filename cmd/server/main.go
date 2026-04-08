package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	brokerUrl = "amqp://guest:guest@localhost:5672/"
)

func main() {
	fmt.Println("Starting Peril server...")

	gamelogic.PrintServerHelp()

	conn, err := amqp.Dial(brokerUrl)
	if err != nil {
		fmt.Printf("Could not connect to RabbitMQ broker: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Println("Server started up successfully!")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	mainCh, err := conn.Channel()
	if err != nil {
		fmt.Printf("Could not create channel: %v\n", err)
		return
	}
	defer mainCh.Close()

	mainCh, topic, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, "game_logs", "game_logs.*", pubsub.Durable)

	fmt.Printf("Topic created: %s\n", topic.Name)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Received exit signal, bye!")
			return
		default:
		}
		// TODO: run submission in boot.dev for CH3-L4 https://www.boot.dev/lessons/dacf0de0-ef47-4343-80cc-7eca3e1c4a4e
		//
		//pubsub.PublishJSON(mainCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		//	IsPaused: true,
		//})
		//

		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}

		switch userInput[0] {
		case "pause":
			fmt.Println("Pausing game session.")
			pubsub.PublishJSON(mainCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
		case "resume":
			fmt.Println("Resume game session.")
			pubsub.PublishJSON(mainCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
		case "quit":
			fmt.Println("Exiting game.")
			return
		default:
			fmt.Printf("Invalid command: %s\n", userInput[0])
			continue
		}
	}
}
