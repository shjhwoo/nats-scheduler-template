package natsutil

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type ScheduledMessage struct {
	Id          string    `json:"id"`
	Content     string    `json:"content"`
	ScheduledAt time.Time `json:"scheduledAt"`
}

func CreateOrUpdateScheduledMessage(scheduledMessage ScheduledMessage) (*jetstream.PubAck, error) {
	// ttl for the message after it reaches the target subject.
	// set this to a value that is long enough for the message to be "Successfully" processed.
	// (including fallback logic - NackWithDelay. refer handleError function in consumer/consumer.go)
	// this can be adjusted based on the expected processing time of the message.
	var processMsgTTL = 180

	pubAck, err := Client.JetStream.PublishMsg(
		context.Background(),
		&nats.Msg{
			Header: nats.Header{
				"Nats-Schedule":        []string{fmt.Sprintf("@at %s", scheduledMessage.ScheduledAt.Format(time.RFC3339))},
				"Nats-Schedule-TTL":    []string{fmt.Sprintf("%ds", processMsgTTL)},
				"Nats-Schedule-Target": []string{fmt.Sprintf("%s.%s", ScheduledMsgProcessSubjectPrefix, scheduledMessage.Id)}, // Target subject
			},
			Subject: fmt.Sprintf("%s.%s", ScheduledMsgPendingSubjectPrefix, scheduledMessage.Id),
			Data:    []byte(scheduledMessage.Content),
		})
	if err != nil {
		return nil, err
	}

	log.Println("scheduled message created/updated successfully, message id: ", scheduledMessage.Id, " scheduled at: ", scheduledMessage.ScheduledAt.Local().Format(time.RFC3339))

	return pubAck, nil
}

func DeleteScheduledMessage(scheduledMessage ScheduledMessage) (*jetstream.PubAck, error) {
	// ttl for the message after it reaches the target subject.
	// this can be short because we will delete the message immediately after it is published to the discard subject.
	var processMsgTTL = 1

	pubAck, err := Client.JetStream.PublishMsg(
		context.Background(),
		&nats.Msg{
			Header: nats.Header{
				"Nats-Schedule":        []string{fmt.Sprintf("@at %s", time.Now().Format(time.RFC3339))}, // this is also required for the message to be deleted from the pending subject. if this is not set, the message will not be deleted and will be processed at the scheduled time.
				"Nats-Schedule-TTL":    []string{fmt.Sprintf("%ds", processMsgTTL)},
				"Nats-Schedule-Target": []string{ScheduledMsgDiscardSubject}, // Target subject
			},
			Subject: fmt.Sprintf("%s.%s", ScheduledMsgPendingSubjectPrefix, scheduledMessage.Id),
			Data:    nil,
		})
	if err != nil {
		return nil, err
	}

	log.Println("scheduled message deleted successfully, message id: ", scheduledMessage.Id)

	return pubAck, nil
}
