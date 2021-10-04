package health

import (
	"context"
	"errors"
	"testing"

	"github.com/ONSdigital/dp-kafka/v3/health/mock"
	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testAddrs = []string{"localhost:12300", "localhost:12301"}
	testTopic = "testTopic"
	ctx       = context.Background()
)

func TestHealthcheck(t *testing.T) {

	Convey("Given that all brokers are available with the expected metadata", t, func() {
		brokers := CreateMockBrokersHappy(t)

		Convey("When Healthcheck is caled", func() {
			err := Healthcheck(ctx, brokers, testTopic, &sarama.Config{})

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then all broker connections are checked and validated", func() {
				for _, broker := range brokers {
					b := broker.(*mock.SaramaBrokerMock)
					So(b.ConnectedCalls(), ShouldHaveLength, 1)
					So(b.GetMetadataCalls(), ShouldHaveLength, 1)
				}
			})
		})
	})

	Convey("Given that a broker connection is closed", t, func() {
		brokers := CreateMockBrokersHappy(t)
		brokers[0].(*mock.SaramaBrokerMock).ConnectedFunc = func() (bool, error) {
			return false, nil
		}
		brokers[0].(*mock.SaramaBrokerMock).OpenFunc = func(conf *sarama.Config) error {
			return nil
		}

		Convey("When Healthcheck is called", func() {
			err := Healthcheck(ctx, brokers, testTopic, &sarama.Config{})

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then all broker connections are checked, broker 0 is reconnected and all are and validated", func() {
				for _, broker := range brokers {
					b := broker.(*mock.SaramaBrokerMock)
					So(b.ConnectedCalls(), ShouldHaveLength, 1)
					So(b.GetMetadataCalls(), ShouldHaveLength, 1)
				}
				So(brokers[0].(*mock.SaramaBrokerMock).OpenCalls(), ShouldHaveLength, 1)
			})
		})
	})

	Convey("Given that a broker connection is closed and it cannot reconnect", t, func() {
		brokers := CreateMockBrokersHappy(t)
		brokers[0] = &mock.SaramaBrokerMock{
			ConnectedFunc: func() (bool, error) {
				return false, nil
			},
			OpenFunc: func(conf *sarama.Config) error {
				return errors.New("cannot reconnect broker")
			},
			GetMetadataFunc: func(request *sarama.MetadataRequest) (*sarama.MetadataResponse, error) {
				return nil, errors.New("cannot get broker metadata")
			},
			AddrFunc: func() string {
				return testAddrs[0]
			},
			CloseFunc: func() error {
				return nil
			},
		}

		Convey("When Healthcheck is called", func() {
			err := Healthcheck(ctx, brokers, testTopic, &sarama.Config{})

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldResemble, "broker(s) not reachable at addresses: [localhost:12300]")
			})

			Convey("Then all broker connections are checked and validated; broker 0 tries to reconnect", func() {
				b := brokers[0].(*mock.SaramaBrokerMock)
				So(b.ConnectedCalls(), ShouldHaveLength, 2)
				So(b.OpenCalls(), ShouldHaveLength, 2)

				for i, broker := range brokers {
					if i == 0 {
						continue
					}
					b := broker.(*mock.SaramaBrokerMock)
					So(b.ConnectedCalls(), ShouldHaveLength, 1)
					So(b.GetMetadataCalls(), ShouldHaveLength, 1)
				}
			})
		})

		Convey("And given that Close also returns an error", func() {
			brokers[0].(*mock.SaramaBrokerMock).CloseFunc = func() error {
				return errors.New("error closing broker")
			}

			Convey("When Healthcheck is called", func() {
				err := Healthcheck(ctx, brokers, testTopic, &sarama.Config{})

				Convey("Then the expected error is returned", func() {
					So(err.Error(), ShouldResemble, "broker(s) not reachable at addresses: [localhost:12300]")
				})
			})
		})

		Convey("And given that 'Connected' also returns an error", func() {
			brokers[0].(*mock.SaramaBrokerMock).ConnectedFunc = func() (bool, error) {
				return false, errors.New("error closing broker")
			}

			Convey("When Healthcheck is called", func() {
				err := Healthcheck(ctx, brokers, testTopic, &sarama.Config{})

				Convey("Then the expected error is returned", func() {
					So(err.Error(), ShouldResemble, "broker(s) not reachable at addresses: [localhost:12300]")
				})
			})
		})
	})

	Convey("Given that all brokers are available and one has a wrong topic metadata", t, func() {
		brokers := CreateMockBrokersHappy(t)
		brokers[1].(*mock.SaramaBrokerMock).GetMetadataFunc = func(request *sarama.MetadataRequest) (*sarama.MetadataResponse, error) {
			return &sarama.MetadataResponse{
				Topics: []*sarama.TopicMetadata{
					{Name: "wrongTopic"},
				},
			}, nil
		}

		Convey("When Healthcheck is called", func() {
			err := Healthcheck(ctx, brokers, testTopic, &sarama.Config{})

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldResemble, "unexpected metadata response for broker(s). Invalid brokers: [localhost:12301]")
			})

			Convey("Then all broker connections are checked and validated", func() {
				for _, broker := range brokers {
					b := broker.(*mock.SaramaBrokerMock)
					So(b.ConnectedCalls(), ShouldHaveLength, 1)
					So(b.GetMetadataCalls(), ShouldHaveLength, 1)
				}
			})
		})
	})

	Convey("Given that Healthcheck is called with an empty list of brokers", t, func() {
		err := Healthcheck(ctx, []SaramaBroker{}, testTopic, &sarama.Config{})

		Convey("Then the expected error is returned", func() {
			So(err.Error(), ShouldResemble, "no brokers defined")
		})
	})
}

// CreateMockBrokersHappy creates mock brokers for testing for a healthy scenario
func CreateMockBrokersHappy(t *testing.T) (brokers []SaramaBroker) {
	for _, addr := range testAddrs {
		mockBroker := &mock.SaramaBrokerMock{}
		mockBroker.AddrFunc = func() string {
			return addr
		}
		mockBroker.ConnectedFunc = func() (bool, error) {
			return true, nil
		}
		mockBroker.GetMetadataFunc = func(request *sarama.MetadataRequest) (*sarama.MetadataResponse, error) {
			return &sarama.MetadataResponse{
				Topics: []*sarama.TopicMetadata{
					{Name: testTopic},
				},
			}, nil
		}
		brokers = append(brokers, mockBroker)
	}
	return brokers
}
