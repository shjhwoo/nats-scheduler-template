# 🚀 Zero-Redis Scheduled Messaging System (Go Template)

> This is the official practical template source code for the **[E-Book] Building a NATS Scheduled Messaging System Without Redis**.
> This repository is a private repository provided exclusively to ebook purchasers.

This template is a Go language boilerplate for building an event-driven, high-volume scheduled messaging system using only **NATS JetStream**, without Redis or heavy DB Polling schedulers.

## ✨ Features

All core patterns covered in Chapter 3 of the ebook are implemented in code.

- **Create Scheduled Message:** Schedule a future message using the `Nats-Schedule` header.
- **Update Scheduled Message:** Modify an existing scheduled message using `MaxMsgsPerSubject`.
- **Cancel Scheduled Message:** Safely cancel sending by changing the Target Subject (`discarded`).
- **Safe Consumption (Consume):** A multi-instance consumer that applies `WorkQueue`, `Durable`, and explicit `Ack` processing.

- **[Bonus] Load Testing Tool:** Includes a load test script to verify the stress of hundreds of reservations and overwrites per second.

## 📂 Directory Structure

```
├── loadTest/
│   └── cmd/
│       └── main.go # starting point of loadtest
│
├── template/
│        ├── apiServer/
│        │   └── apiServer.go # provides http endpoint that helps publish scheduledMsg (you can modify this as a websocket server)
│        ├── cmd/
│        │   └── main.go # starting point
│        ├── internal/
│        │   ├── config/
│        │   │   └── config.go # load and set env
│        │   └── natsutil/
│        │       ├── client.go # provides nats connection, jetstream, consumer creation function
│        │       ├── consumer.go # provides consume function for msgs coming from jetstream
│        │       └── publisher.go # provides helper function for apiServer
│        ├── .env
│        ├── go.mod
│        └── go.sum
│
├── LICENSE.md
└── README.md
```

## simple hands-on with template to understand nats delayed scheduling system

1. run nats server
   this requires nats server version >= 2.12.2. you can run nats-server with docker:

```
docker run -p 4222:4222 -p 8222:8222 -ti nats:latest -js
```

2. run Template service

```
cd ./template

go run ./cmd
```

3. create scheduledMsg that is published after 10 seconds and get it

```bash
curl --location 'http://localhost:8080/scheduledMsg' \
--header 'Content-Type: application/json' \
--data-raw '{
    "id": "schId1",
    "content": "hello world",
    "scheduledAt": "'$(date -u -d "+10 seconds" +"%Y-%m-%dT%H:%M:%SZ")'"
}'

[curl response]
{"message":"Scheduled message created/updated successfully","pubAck":{"stream":"SCHEDULER_STREAM","seq":1}}

[server result]
2026/02/21 13:21:46 scheduled message created/updated successfully, message id:  schId1  scheduled at:  2026-02-21T13:21:56+09:00
[GIN] 2026/02/21 - 13:21:46 | 200 |      3.5691ms |       127.0.0.1 | POST     "/scheduledMsg"
2026/02/21 13:21:56 received message at scheduled time, subject: scheduler.process.schId1, headers: map[Nats-Schedule-Next:[purge] Nats-Scheduler:[scheduler.pending.schId1] Nats-TTL:[180s]], receivedAt: 2026-02-21T13:21:56+09:00, payload: hello world
```

4. update scheduledMsg in scheduler stream with new scheduledAt and content

```bash
curl --location 'http://localhost:8080/scheduledMsg' \
--header 'Content-Type: application/json' \
--data-raw '{
    "id": "schId1",
    "content": "hello world",
    "scheduledAt": "'$(date -u -d "+10 seconds" +"%Y-%m-%dT%H:%M:%SZ")'"
}'

curl --location --request PATCH 'http://localhost:8080/scheduledMsg' \
--header 'Content-Type: application/json' \
--data-raw '{
    "id": "schId1",
    "content": "hello world updated",
    "scheduledAt": "'$(date -u -d "+20 seconds" +"%Y-%m-%dT%H:%M:%SZ")'"
}'

[curl response]
{"message":"Scheduled message created/updated successfully","pubAck":{"stream":"SCHEDULER_STREAM","seq":1}}
{"message":"Scheduled message created/updated successfully","pubAck":{"stream":"SCHEDULER_STREAM","seq":2}}

[server result]
2026/02/21 13:22:57 scheduled message created/updated successfully, message id:  schId1  scheduled at:  2026-02-21T13:23:07+09:00
[GIN] 2026/02/21 - 13:22:57 | 200 |      1.3333ms |       127.0.0.1 | POST     "/scheduledMsg"
2026/02/21 13:23:03 scheduled message created/updated successfully, message id:  schId1  scheduled at:  2026-02-21T13:23:23+09:00
[GIN] 2026/02/21 - 13:23:03 | 200 |      1.5236ms |       127.0.0.1 | PATCH    "/scheduledMsg"
2026/02/21 13:23:23 received message at scheduled time, subject: scheduler.process.schId1, headers: map[Nats-Schedule-Next:[purge] Nats-Scheduler:[scheduler.pending.schId1] Nats-TTL:[180s]], receivedAt: 2026-02-21T13:23:23+09:00, payload: hello world updated
```

5. delete scheduledMsg from scheduler stream before sending

```bash
curl --location 'http://localhost:8080/scheduledMsg' \
--header 'Content-Type: application/json' \
--data-raw '{
    "id": "schId1",
    "content": "hello world",
    "scheduledAt": "'$(date -u -d "+10 seconds" +"%Y-%m-%dT%H:%M:%SZ")'"
}'

curl --location --request DELETE 'http://localhost:8080/scheduledMsg' \
--header 'Content-Type: application/json' \
--data-raw '{
    "id": "schId1"
}'

[curl response]
{"message":"Scheduled message created/updated successfully","pubAck":{"stream":"SCHEDULER_STREAM","seq":1}}
{"message":"Scheduled message deleted successfully","pubAck":{"stream":"SCHEDULER_STREAM","seq":2}}

[server result]
2026/02/21 13:27:04 scheduled message created/updated successfully, message id:  schId1  scheduled at:  2026-02-21T13:27:14+09:00
[GIN] 2026/02/21 - 13:27:04 | 200 |      1.3484ms |       127.0.0.1 | POST     "/scheduledMsg"
2026/02/21 13:27:05 scheduled message deleted successfully, message id:  schId1
[GIN] 2026/02/21 - 13:27:05 | 200 |       1.335ms |       127.0.0.1 | DELETE   "/scheduledMsg"
```

## how to run load test

## 🚀 Load Testing Guide

This template includes a built-in load testing script to verify the system's limits.
It simulates an extreme stress environment by triggering **multiple overwrite (update) requests for each scheduled message** before dispatching.

### 1. How to Run

Execute the test via the `loadtest/main.go` file (adjust the path if necessary).
and look through loadtest/loadResult.log file for NATS cpu and mem usage.

```bash
docker run -p 4222:4222 -p 8222:8222 -ti nats:latest -js -m 8222

cd ./loadtest

# Example: ./start.sh 2000 10 1 100
# (total 2,000 messages, updated 10 times each, dispatching 100 messages every 1 minute)
./start.sh {TOTAL_MESSAGES} {OVERWRITE_COUNT} {INTERVAL_MINUTES} {MESSAGES_PER_INTERVAL}
```

### 2. Monitoring Logs

During the test, two log files will be generated, recording resources every 30 seconds:

loadResult.log: Resource usage of the Scheduler JetStream.

### 3. 📊 Benchmark Results

OS: Windows 11
NATS: (docker container) single node, 3 consumer
total Msgs: 2000
update count per msg: 10
msg scheduleTine interval: 60s

Under severe conditions (each message updated 10 times before scheduling),
NATS handled the traffic flawlessly while maintaining under 1% CPU usage.

| Messages / Min | Avg Latency | Max Latency | NATS CPU | NATS Memory |
| -------------- | ----------- | ----------- | -------- | ----------- |
| 500            | -412.88ms   | 174.126ms   | 0.22%    | 34.21MB     |
| 1000           | -321.84ms   | 345.929ms   | 0.33%    | 34.08 MB    |
| 2000           | 423.670ms   | 1.6536s     | 0.25%    | 30.0 MB     |

🔥 Stress Test (20,000 messages / min):
Even under an extreme load of hundreds of dispatches per second, the system did NOT crash.
It queued and processed all messages sequentially without data loss,
with an average latency of 471.787712ms (max 2.1100791s).
(used nats cpu 17.13%, mem 43.48mb)
