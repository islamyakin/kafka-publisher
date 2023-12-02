package main

import (
	"bufio"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
)

func sendLogLines(logFile string, producer *kafka.Producer, topic string) error{
	file, err := os.Open(logFile)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("Failed to close log file: %s", err)
		}
	}(file)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(line),
		}, nil)
		if err != nil {
			return fmt.Errorf("failed to send message to Kafka: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading log file: %w", err)
	}

	// Tunggu pengiriman pesan selesai
	producer.Flush(15 * 1000)
return nil
}
