package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

/*

NumScheduledMessages:
the total number of messages to be scheduled in the load test.
It will be divided by ModifyCountPerMessage to determine how many unique schedule IDs will be used in the test.

ModifyCountPerMessage:
the number of times each message will be modified with a new scheduled time.
It will determine how many times each schedule ID will be reused in the test.
The scheduled time for each modification will be calculated based on the initial scheduled time and the InitDelayTime.

InitDelayTime:
the initial delay time (in minutes) for the first scheduled time of each message.
It will be used to calculate the scheduled time for each modification of the message by subtracting the ModifyCountPerMessage from it.

ScheduleTimeIntervalMin:
the time interval (in minutes) between the scheduled times of messages.
It will be multiplied by the group number to determine the scheduled time for each message.

ScheduledMsgGroupingCount:
the number of messages that will be grouped together to have the same scheduled time.
It will be used to calculate the scheduled time for each message by dividing the index of the message by it to determine the group number.

*/

var NumScheduledMessages = 2000
var ModifyCountPerMessage = 10
var InitDelayTime int
var ScheduleTimeIntervalMin = 1
var ScheduledMsgGroupingCount = 100

var latencyChan = make(chan time.Duration, 100)

const natsURL = "nats://localhost:4222"
const StreamName = "schedulerStream"
const pendingSubjectPrefix = "scheduler.pending"
const onProcessSubjectPrefix = "scheduler.process"

var natsClient NatsClient

type NatsClient struct {
	Conn               *nats.Conn
	JetStream          jetstream.JetStream
	Stream             jetstream.Stream
	Consumer           jetstream.Consumer
	ConsumeContextList []jetstream.ConsumeContext
}

var wg sync.WaitGroup

var FirstScheduledAt time.Time
var LastScheduledAt time.Time
var totalLatency time.Duration
var maxLatency time.Duration
var messageCount int64

func main() {
	setLoadTestArguments()

	SetupNatsClient()

	defer natsClient.Conn.Close()

	go latencyAggregator()

	wg.Add(NumScheduledMessages)

	publishScheduledMessageToSchedulerStream()

	log.Println("--- all scheduled messages set to pending state and ready to consume ---")

	wg.Wait()

	log.Println("--- all msgs received. close channel ---")
	close(latencyChan)

	log.Println("load test finished, FirstScheduledAt:", FirstScheduledAt, "LastScheduledAt:", LastScheduledAt)
	schDuration := LastScheduledAt.Sub(FirstScheduledAt)

	if schDuration.Minutes() > 0 {
		log.Printf("pub/sub %f scheduled msgs per minute:", float64(NumScheduledMessages)/schDuration.Minutes())
	}

	avgLatency := totalLatency / time.Duration(messageCount)
	log.Println("📢 Latency Aggregator - avgLatency: ", avgLatency, "maxLatency", maxLatency, "count: ", messageCount)
}

func setLoadTestArguments() error {
	args := os.Args

	if len(args) >= 5 {
		numScheduledChatMessagesInt, err := strconv.Atoi(args[1])
		if err != nil {
			return err
		}
		NumScheduledMessages = numScheduledChatMessagesInt

		modifyCountPerMessageInt, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		ModifyCountPerMessage = modifyCountPerMessageInt

		InitDelayTime = ModifyCountPerMessage

		scheduleTimeIntervalMinInt, err := strconv.Atoi(args[3])
		if err != nil {
			return err
		}
		ScheduleTimeIntervalMin = scheduleTimeIntervalMinInt

		scheduledMsgGroupingCountInt, err := strconv.Atoi(args[4])
		if err != nil {
			return err
		}
		ScheduledMsgGroupingCount = scheduledMsgGroupingCountInt
	}

	return nil
}

func SetupNatsClient() error {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return err
	}

	natsClient.Conn = nc

	js, err := jetstream.New(natsClient.Conn)
	if err != nil {
		return err
	}

	natsClient.JetStream = js

	if err := createSchedulerStream(); err != nil {
		return err
	}

	if err := createSchedulerConsumer(); err != nil {
		return err
	}

	return nil
}

func createSchedulerStream() error {
	stream, err := natsClient.JetStream.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:    StreamName,
		Storage: jetstream.FileStorage,
		Subjects: []string{
			fmt.Sprintf("%s.*", pendingSubjectPrefix),
			fmt.Sprintf("%s.*", onProcessSubjectPrefix),
		},
		Retention:         jetstream.WorkQueuePolicy,
		Discard:           jetstream.DiscardOld,
		MaxMsgsPerSubject: 1,
		AllowMsgSchedules: true,
		AllowMsgTTL:       true,
		Replicas:          1,
	})
	if err != nil {
		return err
	}

	log.Println("스트림생성: ",
		fmt.Sprintf("%s.*", pendingSubjectPrefix),
		fmt.Sprintf("%s.*", onProcessSubjectPrefix))

	natsClient.Stream = stream

	streamInfo, err := natsClient.JetStream.Stream(context.Background(), StreamName)
	if err != nil {
		return err
	}

	log.Printf("📢Stream state: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)

	return nil
}

func createSchedulerConsumer() error {
	consumer, err := natsClient.Stream.CreateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:        "scheduledEventSinkConsumer",
		FilterSubjects: []string{fmt.Sprintf("%s.*", onProcessSubjectPrefix)},
		AckPolicy:      jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return err
	}

	natsClient.Consumer = consumer

	ctxList := make([]jetstream.ConsumeContext, 0)

	for range 3 {
		consumeCtx, err := natsClient.Consumer.Consume(func(msg jetstream.Msg) {

			defer msg.Ack()

			var content MessageContent
			err := json.Unmarshal(msg.Data(), &content)
			if err != nil {
				log.Fatalf("failed to unmarshal: %v", err)
			}

			latency := time.Since(content.ScheduledAt)
			latencyChan <- latency
			wg.Done()
			log.Printf("calculated latency: now: %v, scheduledAt : %v,  Latency: %v", time.Now(), content.ScheduledAt, latency)

		})
		if err != nil {
			return err
		}
		ctxList = append(ctxList, consumeCtx)
	}

	natsClient.ConsumeContextList = ctxList

	return nil
}

func latencyAggregator() {

	for latency := range latencyChan {

		log.Println("latency", latency)

		if latency > maxLatency {
			maxLatency = latency
			log.Printf("🚨 max Latency updated: %s", maxLatency)
		}

		totalLatency += latency

		messageCount++
	}

}

type MessageContent struct {
	Id          string
	ScheduledAt time.Time
}

func publishScheduledMessageToSchedulerStream() {
	for idx := range NumScheduledMessages {

		var scheduleId = fmt.Sprintf("SCH_ID_%d", idx+1)

		fastestScheduledAt := getFastestScheduledAtbyIdx(idx)

		for mc := 1; mc <= ModifyCountPerMessage; mc++ {

			scheduledAt := fastestScheduledAt.Add(time.Duration(InitDelayTime-mc) * time.Minute)

			if idx == 0 && mc == ModifyCountPerMessage {
				FirstScheduledAt = scheduledAt
			}

			if idx == NumScheduledMessages-1 && mc == ModifyCountPerMessage {
				LastScheduledAt = scheduledAt
			}

			remainingTime := int(time.Until(scheduledAt).Seconds())

			msg := MessageContent{
				Id:          scheduleId,
				ScheduledAt: scheduledAt,
			}

			msgBytes, err := json.Marshal(msg)
			if err != nil {
				log.Printf("failed to marshal: %v", err)
				continue
			}

			pubAck, err := natsClient.JetStream.PublishMsg(context.Background(), &nats.Msg{
				Header: nats.Header{
					"Nats-Schedule":        []string{fmt.Sprintf("@at %s", scheduledAt.Format(time.RFC3339))},
					"Nats-Schedule-TTL":    []string{"180s"},
					"Nats-Schedule-Target": []string{fmt.Sprintf("%s.%s", onProcessSubjectPrefix, scheduleId)},
				},
				Subject: fmt.Sprintf("%s.%s", pendingSubjectPrefix, scheduleId),
				Data:    msgBytes,
			})
			if err != nil {
				log.Printf("failed to publish msg to scheduler stream: %v, remainingTime: %d", err, remainingTime)
				continue
			}

			if mc == ModifyCountPerMessage {
				log.Println("targetSubject: ", fmt.Sprintf("%s.%s", onProcessSubjectPrefix, scheduleId))
				log.Printf("final version of scheduled msg(%s) published: %+v, scheduledAt: %s (remaining %ds)", scheduleId, pubAck, scheduledAt.Format(time.RFC3339), remainingTime)
			}
		}
	}

	PrintStreamInfo()
}

func getFastestScheduledAtbyIdx(idx int) time.Time {

	fatestNextMinuteTime := getFatestNextMinuteTime()

	d := idx / ScheduledMsgGroupingCount

	add := ScheduleTimeIntervalMin * (d + 1)

	return fatestNextMinuteTime.Add(time.Duration(add) * time.Minute)
}

func getFatestNextMinuteTime() time.Time {
	currentTime := time.Now()
	currentSec := currentTime.Second()
	currentTimeWithoutSec := currentTime.Add(time.Duration(-currentSec) * time.Second)
	return currentTimeWithoutSec.Add(1 * time.Minute)
}

func PrintStreamInfo() {
	streamInfo, err := natsClient.JetStream.Stream(context.Background(), StreamName)
	if err == nil {
		log.Printf("📢 Stream state: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)
	}
}
