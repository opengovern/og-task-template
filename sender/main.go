package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/opengovern/og-util/pkg/jq"

	"go.uber.org/zap"
)

// define a nats stream queue and topic name constants

var (
	// Stream name
	StreamName = os.Getenv("STREAM_NAME")
	// Queue name
	QueueName = os.Getenv("QUEUE_NAME")
	// Topic name Reciever
	TopicNameReciever = os.Getenv("TOPIC_NAME")
	// Nats server url
	NatsURL = os.Getenv("NATS_URL")
	// Topic name Sender
	TopicNameSender = os.Getenv("TOPIC_NAME_SENDER")

)

func main() {
	// create a nats connection
	// consume the message from the queue with task id then send the result to it
	// the message will be sent to the topic
	logger,err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to create logger: %v", err)
	}
	jqClient, err := jq.New(NatsURL, logger)
	if err != nil {
		logger.Fatal("failed to create jq", zap.Error(err))
	}
	// create new context
	ctx := context.Background()
	_, err = jqClient.Consume(ctx,"tasks",StreamName,[]string{TopicNameReciever},QueueName,func(msg jetstream.Msg){
		if err := msg.Ack(); err != nil {
				logger.Error("Failed committing message", zap.Error(err))
			}
			var TaskId string
			if err := json.Unmarshal(msg.Data(), &TaskId); err != nil {
				logger.Error("Failed to unmarshal ComplianceReportJob results", zap.Error(err))
				return

			}
			logger.Info("Processing task result",
				zap.String("task_id", TaskId),)

			// send the result to the topic
			// read the resulst.txt file
			file, err := os.Open("output.txt")
			if err != nil {
				log.Fatalf("failed to open file: %v", err)
			}
			defer file.Close()
			// read the file
			data := make([]byte, 1024*1024*10)
			count, err := file.Read(data)
			if err != nil {
				log.Fatalf("failed to read file: %v", err)
			}
			// send the result to the topic
			_,err = jqClient.Produce(ctx, TopicNameSender, data[:count], TaskId)
			if err != nil {
				logger.Error("Failed to publish ComplianceReportJob results", zap.Error(err))
			}
			logger.Info("Published ComplianceReportJob results",
				zap.String("task_id", TaskId))
	})

	if err != nil {
		return 
	}
	
}

