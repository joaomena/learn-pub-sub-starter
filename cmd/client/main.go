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
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial(brokerUrl)
	if err != nil {
		fmt.Printf("Could not connect to RabbitMQ broker: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Println("Client started up successfully!")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	defer fmt.Println("Received exit signal, bye!")

	username, err := gamelogic.ClientWelcome()
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)

	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		fmt.Printf("Could not declare and bind queue: %v\n", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		gamestate := gamelogic.NewGameState(username)

		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}

		switch userInput[0] {
		case "spawn":
			gamestate.CommandSpawn(userInput)
		case "move":
			_, err := gamestate.CommandMove(userInput)
			if err != nil {
				fmt.Println("Error while moving: %v. Please try again...", err)
				continue
			}
			fmt.Println("Moved successfully")
		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet.")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Invalid command.")
			continue
		}
	}
}
