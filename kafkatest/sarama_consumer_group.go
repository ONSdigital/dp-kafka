package kafkatest

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/ONSdigital/dp-kafka/v4/interfaces"
	"github.com/ONSdigital/dp-kafka/v4/mock"
)

// NewSaramaConsumerGroupSessionMock returns a new sarama consuemr group session mock with the provided number of partitions
// it also returns a func to cancel the context, to be used when a session is ending
func NewSaramaConsumerGroupSessionMock(memberID, topic string, numPartitions int) (*mock.SaramaConsumerGroupSessionMock, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	part := make([]int32, numPartitions)
	var i int32
	for i = 0; i < int32(numPartitions); i++ {
		part[i] = i + 1
	}
	claims := map[string][]int32{
		topic: part,
	}

	return &mock.SaramaConsumerGroupSessionMock{
		ClaimsFunc: func() map[string][]int32 {
			return claims
		},
		ContextFunc: func() context.Context {
			return ctx
		},
		MemberIDFunc: func() string {
			return memberID
		},
		MarkMessageFunc: func(msg *sarama.ConsumerMessage, metadata string) {
		},
		CommitFunc: func() {
		},
	}, cancel
}

func NewSaramaConsumerGroupClaimMock(ch chan *sarama.ConsumerMessage) *mock.SaramaConsumerGroupClaimMock {
	return &mock.SaramaConsumerGroupClaimMock{
		MessagesFunc: func() <-chan *sarama.ConsumerMessage {
			return ch
		},
	}
}

func SaramaBrokerGenerator(topic string) func(addr string) interfaces.SaramaBroker {
	return func(addr string) interfaces.SaramaBroker {
		return &mock.SaramaBrokerMock{
			AddrFunc: func() string {
				return addr
			},
			ConnectedFunc: func() (bool, error) {
				return true, nil
			},
			OpenFunc: func(conf *sarama.Config) error {
				return nil
			},
			GetMetadataFunc: func(request *sarama.MetadataRequest) (*sarama.MetadataResponse, error) {
				return &sarama.MetadataResponse{
					Topics: []*sarama.TopicMetadata{
						{Name: topic},
					},
				}, nil
			},
			CloseFunc: func() error {
				return nil
			},
		}
	}
}
