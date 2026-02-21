package natsutil

import (
	"log"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// HandleScheduledMessage is the callback function that will be called
// when a scheduled message is received.
func (nc *NatsClient) HandleScheduledMessage(msg jetstream.Msg) {
	var msgHandlingErr error

	defer nc.handleError(msg, msgHandlingErr)

	log.Printf("received message at scheduled time, subject: %s, headers: %v, receivedAt: %s, payload: %s ",
		msg.Subject(),
		msg.Headers(),
		time.Now().Format(time.RFC3339),
		string(msg.Data()),
	)

	//TODO: implement message handling logic here
}

func (nc *NatsClient) handleError(msg jetstream.Msg, err error) {
	if err != nil {
		// If there is an error handling the message,
		// we can choose to either retry processing the message after some delay
		// or discard the message.
		// so for this case, it is recommended to set Nats-Schedule-TTL header to a value that is long enough for the message
		// to be retried a few times before it is discarded by the consumer.
		msg.NakWithDelay(time.Duration(5) * time.Second)
	} else {
		// If the message is processed successfully,
		// we acknowledge the message to remove it from the stream.
		msg.Ack()
	}
}
