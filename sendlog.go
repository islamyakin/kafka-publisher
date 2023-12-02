package main

import (
	"bufio"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
)

func sendLogLines(logFile string, producer *kafka.Producer, topic string) {
	file, err := os.Open(logFile)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(line),
		}, nil)
	}
	if err != nil {
		log.Printf("Failed to send message to Kafka: %s\n", err)
	} else {
		log.Printf("Message sent to Kafka %s", producer.String())
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading log file: %s", err)
	}

	// Tunggu pengiriman pesan selesai
	producer.Flush(15 * 1000)

}
