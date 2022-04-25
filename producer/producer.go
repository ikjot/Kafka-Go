package main

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
)

func main() {
	cmt := 123456
	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		fmt.Println("Error while json marshalling")
	}

	error := PushCommentToQueue("comments", cmtInBytes)
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

func PushCommentToQueue(topic string, message []byte) error {

	brokersUrl := []string{"127.0.0.1:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		fmt.Println("Error while ConnectProducer")
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	fmt.Println(msg.Topic)
	fmt.Println(msg.Value)
	fmt.Println(msg.Timestamp)

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error while SendMessage")
		fmt.Println(err.Error())

		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}
