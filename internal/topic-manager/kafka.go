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

	//ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	//defer cancel()
	//_, err = client.CreateTopics(ctx, []kafka.TopicSpecification{{
	//	Topic:             "test",
	//	NumPartitions:     1,
	//	ReplicationFactor: 1,
	//	Config: map[string]string{
	//		"compression.type": "gzip",
	//	},
	//}})
	//if err != nil {
	//	panic(err)
	//}
	//
	//res, err := client.DescribeConfigs(ctx, []kafka.ConfigResource{{
	//	Type: kafka.ResourceTopic,
	//	Name: "testt",
	//}})
	//
	//if err != nil {
	//	panic(err)
	//}
	//
	//fmt.Printf("result: %+v", res[0].Error)
	//kafka.ErrUnknownTopicOrPart
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
			return nil
		}
		return fmt.Errorf("can't fetch topic configuration: %d", kafkaErr.Code())
	}

	return fmt.Errorf("response error should not be empty")

	//_, err := tm.client.CreateTopics(ctx, []kafka.TopicSpecification{{
	//	Topic:             name,
	//	NumPartitions:     1,
	//	ReplicationFactor: 1,
	//	Config: map[string]string{
	//		"compression.type": "gzip",
	//	},
	//}})
	//if err != nil {
	//	panic(err)
	//}
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

func (tm *TopicManager) Close() {
	tm.client.Close()
}
