package rabbitmq

import "testing"

func TestNewConnection(t *testing.T) {
	conn := NewConnection("conn1", "", false)
	conn.Connect("amqp://localhost")
	err := conn.BindQueue("test", true, false ,false)
	msgs := conn.Consume("test")
	if err != nil {
		t.Error(err)
	}
}
