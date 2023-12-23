package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/fsnotify/fsnotify"
	"github.com/joho/godotenv"
	"log"
	"os"
)

func loadEnv() error {
	return godotenv.Load()
}
func getEnvVars() (string, string) {
	return os.Getenv("LOG_FILE"), os.Getenv("KAFKA_TOPIC")
}

func createKafkaProducer(topic string) (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  os.Getenv("KAFKA_BROKERS"),
		"enable.idempotence": os.Getenv("KAFKA_IDEMPOTENCE"),
	})
	if err != nil {
		return nil, err
	}
	log.Printf("kafka producer created with topic: %s", topic)
	return producer, nil
}
func setupFileWatcher(logFile string, producer *kafka.Producer, topic string) (*fsnotify.Watcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					log.Printf("Detected new log entry on %s, sending to Kafka...", event.Name)
					sendLogLines(logFile, producer, topic)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	err = watcher.Add(logFile)
	if err != nil {
		return nil, err
	}
	log.Printf("Started watching file: %s", logFile)

	return watcher, nil
}
func main() {
	err := loadEnv()
	if err != nil {
		log.Fatalf("Failed to load env: %s", err)
	}

	logFile, topic := getEnvVars()

	producer, err := createKafkaProducer(topic)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	watcher, err := setupFileWatcher(logFile, producer, topic)
	if err != nil {
		log.Fatalf("Failed to setup watcher: %s", err)
	}
	defer func(watcher *fsnotify.Watcher) {
		err := watcher.Close()
		if err != nil {
			log.Fatalf("Failed to close watcher: %s", err)
		}
	}(watcher)

	waitForEvents()
}
func waitForEvents() {
	done := make(chan bool)
	<-done
}
