package main

import (
	"fmt"
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"os"
)

var svc = getSQSInstance()
var qURL = getQueueUrl(svc, "ttv-search-asset-clicks-out")

type MyEvent struct {
	Body string `json:"Body"`
	MessageId string `json:"MessageId"`
}

func handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	for _, message := range sqsEvent.Records {
		fmt.Printf("The message %s for event source %s = %s \n", message.MessageId, message.EventSource, message.Body)
		send(message.Body)
	}

	return nil
}


func send(msg string)  {
	send_params := &sqs.SendMessageInput{
		MessageBody:  aws.String(msg), // Required
		QueueUrl:     aws.String(qURL),
		DelaySeconds: aws.Int64(3),
	}
	send_resp, err := svc.SendMessage(send_params)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("[Send message] \n%v \n\n", send_resp)
}

func main() {
	lambda.Start(handler)
}

func getSQSInstance() *sqs.SQS {

	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(endpoints.ApSoutheast2RegionID),
	}))
	return sqs.New(sess)
}

func getQueueUrl(svc *sqs.SQS, qName string) string {
	result, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(qName),
	})
	if err != nil {
		fmt.Println("Error", err)
		os.Exit(1)
	}
	return *result.QueueUrl
}