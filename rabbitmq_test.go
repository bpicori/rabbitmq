package rabbitmq

import (
	"testing"
)

func TestNewConnection(t *testing.T) {
	c1 := NewConnection("connection1", "amqp://localhost", "")
	t.Run("should create a conn struct", func(t *testing.T) {
		if c1.name != "connection1" || c1.uri != "amqp://localhost" || c1.exchange != "" {
			t.Errorf("error asserting connection struct")
		}
	})
	t.Run("should connect with rabbit with default exchange", func(t *testing.T) {
		err := c1.Connect()
		if err != nil {
			t.Errorf("error connecting with rabbitmq %v", err)
		}
	})
	t.Run("should connect with rabbit with custom exchange exchange", func(t *testing.T) {
		c2 := NewConnection("connection2", "amqp://localhost", "test-exchange")
		err := c2.Connect()
		if err != nil {
			t.Errorf("error connecting with rabbitmq %v", err)
		}
	})
	t.Run("should create channel when connects", func(t *testing.T) {
		c3 := NewConnection("connection3", "amqp://localhost", "test-exchange")
		err := c3.Connect()
		if err != nil {
			t.Errorf("error connecting with rabbitmq %v", err)
		}
		if c3.Channel == nil {
			t.Errorf("channel is not created")
		}

	})
}
