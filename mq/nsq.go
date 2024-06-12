package mq

import (
	"errors"
	"time"

	"github.com/hjwlsy/frame/tool"
	"github.com/nsqio/go-nsq"
)

type PConfig struct {
	Server string
	Topic  string
}

type Producer struct {
	topic string
	conn  *nsq.Producer
}

func NewProducer(c PConfig) (*Producer, error) {
	if len(c.Server) == 0 || len(c.Topic) == 0 {
		return nil, errors.New("config deficiency")
	}
	conn, err := nsq.NewProducer(c.Server, nsq.NewConfig())
	if err != nil {
		return nil, err
	}
	conn.SetLoggerLevel(nsq.LogLevelError)
	if err = conn.Ping(); err != nil {
		return nil, err
	}
	return &Producer{topic: c.Topic, conn: conn}, nil
}

func (p *Producer) Publish(msg string, delay ...time.Duration) error {
	if len(delay) > 0 && delay[0] > 0 {
		if delay[0] > time.Hour { //nsq延时时间0-3600,000ms
			delay[0] = time.Hour
		}
		return p.conn.DeferredPublish(p.topic, delay[0], tool.String2Bytes(msg))
	}
	return p.conn.Publish(p.topic, tool.String2Bytes(msg))
}

func (p *Producer) Stop() {
	p.conn.Stop()
}

type CConfig struct {
	Server  string
	Topic   string
	Channel string
	Handle  func(msg string, args ...string) error
}

type Handle struct {
	handle  func(msg string, args ...string) error
	topic   string
	channel string
}

func (c *Handle) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		return nil
	}
	return c.handle(string(m.Body), c.topic, c.channel)
}

type Consumer struct {
	conn *nsq.Consumer
}

func (c *Consumer) Stop() {
	c.conn.Stop()
}

func NewConsumer(c CConfig) (*Consumer, error) {
	if len(c.Server) == 0 || len(c.Topic) == 0 || len(c.Channel) == 0 || c.Handle == nil {
		return nil, errors.New("config deficiency")
	}
	conn, err := nsq.NewConsumer(c.Topic, c.Channel, nsq.NewConfig())
	if err != nil {
		return nil, err
	}
	conn.SetLoggerLevel(nsq.LogLevelError)
	conn.AddHandler(&Handle{handle: c.Handle, topic: c.Topic, channel: c.Channel})
	err = conn.ConnectToNSQLookupd(c.Server)
	if err != nil {
		return nil, err
	}
	return &Consumer{conn: conn}, nil
}
