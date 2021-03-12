package topic_manager

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type TopicManager struct {
	client *kafka.AdminClient
}

func New() *TopicManager {
	client, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	return &TopicManager{client: client}
}

func (tm *TopicManager) UpsertTopic(name string, partitions int, replicas int, config map[string]string) error {
	// create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// check if topic exists by trying to fetch config
	res, err := tm.client.DescribeConfigs(ctx, []kafka.ConfigResource{{
		Type: kafka.ResourceTopic,
		Name: name,
	}})

	if err != nil {
		return fmt.Errorf("can't receive config from kafka: %v", err)
	}

	if len(res) != 1 {
		return fmt.Errorf("unexpected response length")
	}

	err = res[0].Error
	if err != nil {
		// cast to kafka.Error
		kafkaErr, ok := err.(kafka.Error)
		// unknown error type
		if !ok {
			return err
		}
		// on unknown topic error the topic does not exists
		if kafkaErr.Code() == kafka.ErrUnknownTopicOrPart || kafkaErr.Code() == kafka.ErrUnknownTopic {
			fmt.Printf("need to create topic\n")
			if err := tm.createTopic(ctx, name, partitions, replicas, config); err != nil {
				return fmt.Errorf("can't create topic: %v", err)
			}
			fmt.Printf("created topic\n")
			return nil
		}

		if kafkaErr.Code() == kafka.ErrNoError {
			fmt.Printf("need to update topic\n")
			//TODO: handle replicas -> can't be updated
			err := tm.updateTopic(ctx, name, partitions, config)
			if err != nil {
				return fmt.Errorf("can't update topic: %v", err)
			}
			return nil
		}
		return fmt.Errorf("can't fetch topic configuration: %d", kafkaErr.Code())
	}

	return fmt.Errorf("response error should not be empty")
}

func (tm *TopicManager) createTopic(ctx context.Context, name string, partitions int, replicas int, config map[string]string) error {
	_, err := tm.client.CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic:             name,
		NumPartitions:     partitions,
		ReplicationFactor: replicas,
		Config:            config,
	}})
	if err != nil {
		return err
	}
	return nil
}

func (tm *TopicManager) updateTopic(ctx context.Context, name string, partitions int, config map[string]string) error {

	var configEntries []kafka.ConfigEntry

	for key, value := range config {
		configEntries = append(configEntries, kafka.ConfigEntry{
			Name:      key,
			Value:     value,
			Operation: kafka.AlterOperationSet,
		})
	}

	_, err := tm.client.AlterConfigs(ctx, []kafka.ConfigResource{{
		Type:   kafka.ResourceTopic,
		Name:   name,
		Config: configEntries,
	}})

	if err != nil {
		return err
	}

	meta, err := tm.client.GetMetadata(&name, false, 10*1000)
	if err != nil {
		return fmt.Errorf("can't fetch metadata for topic %s: %v", name, err)
	}

	//TODO: could meta.Topics[name] be nil?
	partitionCount := len(meta.Topics[name].Partitions)
	if partitionCount > partitions {
		return fmt.Errorf("can't reduce partition count")
	}

	_, err = tm.client.CreatePartitions(ctx, []kafka.PartitionsSpecification{{
		Topic:      name,
		IncreaseTo: partitions,
	}})

	if err != nil {
		return fmt.Errorf("can't create new partitions: %v", err)
	}

	return nil
}

func (tm *TopicManager) DeleteTopic(name string) error {
	// create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := tm.client.DeleteTopics(ctx, []string{name})
	if err != nil {
		return err
	}
	return nil
}

func (tm *TopicManager) Close() {
	tm.client.Close()
}
