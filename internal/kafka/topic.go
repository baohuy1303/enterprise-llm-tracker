package kafka

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

// EnsureTopic creates the topic with the given partition count if it doesn't
// already exist. Idempotent — re-running this on an existing topic is a no-op.
// Sets a 7-day retention so events are replayable for a week after publish.
func EnsureTopic(brokers []string, topic string, partitions int) error {
	if len(brokers) == 0 {
		return errors.New("no kafka brokers configured")
	}

	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial broker %s: %w", brokers[0], err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("read controller: %w", err)
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafka.Dial("tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("dial controller %s: %w", controllerAddr, err)
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "retention.ms", ConfigValue: "604800000"}, // 7 days
		},
	})
}
