package natsutil

import (
	"context"
	"fmt"
	"log"

	"nats_scheduler_template/internal/config"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var Client *NatsClient

type NatsClient struct {
	Conn            *nats.Conn
	JetStream       jetstream.JetStream
	SchedulerStream jetstream.Stream
	Consumer        jetstream.Consumer
	ConsumerContext jetstream.ConsumeContext
}

var StreamName = "SCHEDULER_STREAM"
var SchedulerConsumerDurableName = "SCHEDULER_CONSUMER"
var ScheduledMsgPendingSubjectPrefix = "scheduler.pending"
var ScheduledMsgProcessSubjectPrefix = "scheduler.process"
var ScheduledMsgDiscardSubject = "scheduler.discarded"

func NewNatsClient() error {

	var result NatsClient
	nc, err := Connect()
	if err != nil {
		return err
	}

	result.Conn = nc

	if err := result.CreateSchedulerStream(); err != nil {
		return err
	}

	if err := result.CreateSchedulerConsumer(); err != nil {
		return err
	}

	Client = &result

	return nil
}

func Connect() (*nats.Conn, error) {
	natsURL := config.Get("NATS_URL", nats.DefaultURL)
	var err error
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}

	log.Println("Connected to NATS server")
	return nc, nil
}

func (nc *NatsClient) CreateSchedulerStream() error {

	js, err := jetstream.New(nc.Conn)
	if err != nil {
		return err
	}

	nc.JetStream = js

	stream, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:    StreamName,
		Storage: jetstream.FileStorage,
		Subjects: []string{
			fmt.Sprintf("%s.*", ScheduledMsgPendingSubjectPrefix),
			fmt.Sprintf("%s.*", ScheduledMsgProcessSubjectPrefix),
			ScheduledMsgDiscardSubject,
		},
		AllowMsgSchedules: true,
		AllowMsgTTL:       true,
		Retention:         jetstream.WorkQueuePolicy,
		MaxMsgsPerSubject: 1,
		Discard:           jetstream.DiscardOld,
		Replicas:          1,
	})
	if err != nil {
		return err
	}

	log.Printf("Scheduler stream created: %s", stream.CachedInfo().Config.Name)

	nc.SchedulerStream = stream

	return nil
}

func (nc *NatsClient) CreateSchedulerConsumer() error {
	consumer, err := nc.SchedulerStream.CreateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:        SchedulerConsumerDurableName,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{fmt.Sprintf("%s.*", ScheduledMsgProcessSubjectPrefix)},
	})
	if err != nil {
		return err
	}

	log.Printf("Scheduler consumer created: %s", consumer.CachedInfo().Name)

	nc.Consumer = consumer

	return nil
}

func (nc *NatsClient) StartSchedulerConsumer() error {

	consumeCtx, err := nc.Consumer.Consume(nc.HandleScheduledMessage)
	if err != nil {
		return err
	}

	log.Println("Scheduler consumer started")

	nc.ConsumerContext = consumeCtx

	return nil
}

func (nc *NatsClient) Close() {
	if nc.Conn != nil {
		nc.ConsumerContext.Drain()
		nc.Conn.Close()

		log.Println("NATS connection closed")
	}
}
