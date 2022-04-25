package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

func main() {

	cmtInBytes, err := json.Marshal("***-Dummy-value-***")
	if err != nil {
		fmt.Println("Error while json marshalling")
	}
	var brokersUrl []string = strings.Split(os.Getenv("BROKERS_URL"), ",")
	topic := os.Getenv("TOPIC_NAME")

	error := PushCommentToQueue(brokersUrl, topic, cmtInBytes)
	if error != nil {
		fmt.Println("Error while pushing comment to Queue")
	} else {
		fmt.Println("Message successfully pushed to Kafka")
	}
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func PushCommentToQueue(brokersUrl []string, topic string, message []byte) error {

	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		fmt.Println("Error while connecting producer")
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error while sending message to Kafka")
		fmt.Println(err.Error())

		return err
	}

	fmt.Printf("Success. Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}
