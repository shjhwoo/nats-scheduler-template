package main

import (
	"log"
	apiserver "nats_scheduler_template/apiServer"
	"nats_scheduler_template/internal/config"
	"nats_scheduler_template/internal/natsutil"
)

func main() {
	config.Load()

	if err := natsutil.NewNatsClient(); err != nil {
		log.Println("failed to start new nats client: ", err)
		return
	}

	if err := natsutil.Client.StartSchedulerConsumer(); err != nil {
		log.Println("failed to start scheduler consumer: ", err)
		return
	}

	apiserver.SetupPublisherAPIServer()
	if err := apiserver.APIServer.Run(":8080"); err != nil {
		log.Println("failed to run publisher API server: ", err)
		return
	}
}
