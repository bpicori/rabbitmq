/** Package rabbitmq TODO
1. create errors variable
2. handle content type header
  * json
  * gzip
3. create rpc handler function
*/

package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"os"
	"sync"
	"time"
)

type RpcMap map[string]chan amqp.Delivery

//MessageBody is the struct for the body passed in the AMQP message. The type will be set on the Request header
type MessageBody struct {
	Data []byte
}

//BindQueueOptions options when creating queue
type BindQueueOptions struct {
	autodelete bool
	exclusive  bool
	durable    bool
	nowait     bool
}

//Message is the amqp request to publish
type Message struct {
	Exchange      string
	Queue         string
	ReplyTo       string
	ContentType   string
	CorrelationID string
	Priority      uint8
	Body          MessageBody
	Timeout       time.Duration
}

//Connection is the connection created
type Connection struct {
	name         string
	uri          string
	conn         *amqp.Connection
	Channel      *amqp.Channel
	exchange     string
	err          chan error
	errorHandler func() error
	rpcStarted   bool
	rpcQueueName string
}

var (
	connectionPool = make(map[string]*Connection)
	rpcMap         RpcMap
	rpcMapMutex    sync.RWMutex
)

//NewConnection returns the new connection struct
func NewConnection(name, uri string, exchange string) *Connection {
	if c, ok := connectionPool[name]; ok {
		return c
	}
	c := &Connection{
		name: name,
		exchange: exchange,
		err:      make(chan error),
		uri:      uri,
	}
	connectionPool[name] = c
	return c
}

//GetConnection returns the connection which was instantiated
func GetConnection(name string) *Connection {
	return connectionPool[name]
}

// Connect to rabbitmq
func (c *Connection) Connect() error {
	var err error
	c.conn, err = amqp.Dial(c.uri)
	if err != nil {
		return fmt.Errorf("error in creating rabbitmq connection with %s : %s", c.uri, err.Error())
	}
	go func() {
		<-c.conn.NotifyClose(make(chan *amqp.Error)) //Listen to NotifyClose
		c.err <- errors.New("connection closed") // TODO error type
	}()
	c.Channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("error creating channel: %v", err)
	}
	if c.exchange != "" {
		if err := c.Channel.ExchangeDeclare(
			c.exchange, // name
			"direct",   // type
			true,       // durable
			false,      // auto-deleted
			false,      // internal
			false,      // noWait
			nil,        // arguments
		); err != nil {
			return fmt.Errorf("error in Exchange Declare: %s", err)
		}
	}
	return nil
}

// DefaultBindQueueOptions config
func DefaultBindQueueOptions() BindQueueOptions {
	return BindQueueOptions{
		autodelete: false,
		exclusive:  false,
		durable:    true,
		nowait:     false,
	}
}

// CreateQueue with exchange
func (c *Connection) CreateQueue(queue string, options BindQueueOptions) error {
	if _, err := c.Channel.QueueDeclare(queue, options.durable, options.autodelete, options.autodelete, options.nowait, nil); err != nil {
		return fmt.Errorf("error in declaring the queue %s", err)
	}
	if err := c.Channel.QueueBind(queue, queue, c.exchange, false, nil); err != nil {
		return fmt.Errorf("Queue  Bind error: %s", err)
	}
	return nil
}

func (c *Connection) Rpc(message Message) (amqp.Delivery, error) {
	var d amqp.Delivery
	if !c.rpcStarted {
		hostname, _ := os.Hostname()
		if err := c.StartRpc(hostname); err != nil {
			return d, fmt.Errorf("error starting rpc %v", err)
		}
	}
	if message.CorrelationID == "" {
		message.CorrelationID = uuid.New().String()
	}
	if message.ReplyTo == "" {
		message.ReplyTo = c.rpcQueueName
	}
	if err := c.Publish(message); err != nil {
		return d, err
	}
	ch := make(chan amqp.Delivery)
	timer := time.NewTicker(message.Timeout)
	rpcMapMutex.Lock()
	rpcMap[message.CorrelationID] = ch
	rpcMapMutex.Unlock()
	for {
		select {
		case <-ch:
			d = <-ch
			return d, nil
		case <-timer.C:
			return d, errors.New("rpc timeout")
		}
	}
}

func (c *Connection) StartRpc(name string) error {
	c.rpcQueueName = fmt.Sprintf("%s.%s", name, uuid.New().String())
	if err := c.CreateQueue(c.rpcQueueName, BindQueueOptions{
		autodelete: true,
		exclusive:  true,
		durable:    true,
		nowait:     false,
	}); err != nil {
		return fmt.Errorf("error in declaring the queue: %s", err)
	}

	msgs, err := c.Consume(c.rpcQueueName)
	if err != nil {
		return err
	}
	rpcMap = make(RpcMap)

	go func(msgs <-chan amqp.Delivery) {
		for d := range msgs {
			fmt.Println("Received msg rpcStarted")
			rpcMapMutex.Lock()
			ch, ok := rpcMap[d.CorrelationId]
			if ok {
				ch <- d
			}
			rpcMapMutex.Unlock()
		}
	}(msgs)
	c.rpcStarted = true
	return nil
}

// Consume queue handler
func (c *Connection) Consume(queue string) (<-chan amqp.Delivery, error) {
	deliveries, err := c.Channel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	return deliveries, nil
}

//HandleConsumedDeliveries handles the consumed deliveries from the queues. Should be called only for a consumer connection
//func (c *Connection) HandleConsumedDeliveries(delivery <-chan amqp.Delivery, fn func(<-chan amqp.Delivery)) {
//	for {
//		go fn(delivery)
//		if err := <-c.err; err != nil {
//			panic("Rabbitmq connection error")
//		}
//	}
//}

// Publish message
func (c *Connection) Publish(message Message) error {
	p := amqp.Publishing{
		Headers:       amqp.Table{},
		ContentType:   message.ContentType,
		CorrelationId: message.CorrelationID,
		Body:          message.Body.Data,
		ReplyTo:       message.ReplyTo,
	}
	exchange := c.exchange
	if message.Exchange != "" {
		exchange = message.Exchange
	}
	if err := c.Channel.Publish(exchange, message.Queue, false, false, p); err != nil {
		return fmt.Errorf("error in Publishing: %s", err)
	}
	return nil
}
