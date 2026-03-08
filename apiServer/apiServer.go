package apiserver

import (
	"nats_scheduler_template/internal/natsutil"

	"github.com/gin-gonic/gin"
)

var APIServer *gin.Engine

func SetupPublisherAPIServer() {
	APIServer = gin.Default()
	APIServer.POST("/scheduledMsg", HandleCreateOrUpdateScheduledMessage)
	APIServer.PATCH("/scheduledMsg", HandleCreateOrUpdateScheduledMessage)
	APIServer.DELETE("/scheduledMsg", HandleDeleteScheduledMessage)
}

func HandleCreateOrUpdateScheduledMessage(c *gin.Context) {
	var scheduledMessage natsutil.ScheduledMessage
	if err := c.ShouldBindJSON(&scheduledMessage); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request body"})
		return
	}

	pubAck, err := natsutil.CreateOrUpdateScheduledMessage(scheduledMessage)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to create/update scheduled message"})
		return
	}

	c.JSON(200, gin.H{"message": "Scheduled message created/updated successfully", "pubAck": pubAck})
}

func HandleDeleteScheduledMessage(c *gin.Context) {
	var scheduledMessage natsutil.ScheduledMessage
	if err := c.ShouldBindJSON(&scheduledMessage); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request body"})
		return
	}

	pubAck, err := natsutil.DeleteScheduledMessage(scheduledMessage)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to delete scheduled message"})
		return
	}

	c.JSON(200, gin.H{"message": "Scheduled message deleted successfully", "pubAck": pubAck})
}
