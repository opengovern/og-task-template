package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/opengovern/og-util/pkg/jq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/runtime/protoimpl"

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
	// GRPC server url
	GRPCServerURL = os.Getenv("GRPC_SERVER_URL")

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
			err = sendGRPC(ctx,data[:count],logger)
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

// function to send data with grpc
type ResponseOK struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}
func sendGRPC(ctx context.Context,data []byte, logger *zap.Logger) error {
	// create a grpc connection
	// send the data to the grpc server
	// the grpc server will save the data to the database
	// create a grpc connection
	grpcCtx := metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{}))
	var client grpc.ClientConnInterface

	logger.Info("Setting grpc connection opts")
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	for retry := 0; retry < 5; retry++ {
		conn, err := grpc.NewClient(
			GRPCServerURL,
			opts...,
		)
		if err != nil {
			logger.Error("[result delivery] connection failure:", zap.Error(err))
			if retry == 4 {
				return err
			}
			time.Sleep(1 * time.Second)
			continue
		}
		client = conn

		break
	}
	for retry := 0; retry < 5; retry++ {
		// send the data to the grpc server
		out := new(ResponseOK)
		 err := client.Invoke(grpcCtx, "/Tasks/Results", data,out)
		if err != nil {
			logger.Error("[result delivery] failed to send result:", zap.Error(err))
		}
			if retry== 4 {
				return err
			}
			time.Sleep(1 * time.Second)
			continue
		}

	return nil




}


