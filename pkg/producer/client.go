package producer

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/rcy1122/msg/common/broker"
	"github.com/rcy1122/msg/pkg/log"
	"github.com/rcy1122/msg/pkg/safe"
	"google.golang.org/grpc"
)

type Config struct {
	Server string `json:"server" yaml:"server"`
	Topic  string `json:"topic" yaml:"topic"`
}

type Client struct {
	ctx context.Context
	cfg *Config
	msg chan string
	broker.BrokerClient
}

func New(ctx context.Context, cfg *Config) (*Client, error) {
	rpc, err := grpc.Dial(cfg.Server, grpc.WithInsecure())
	if err != nil {
		return nil, errors.WithMessage(err, "dial")
	}
	c := &Client{
		ctx:          ctx,
		cfg:          cfg,
		msg:          make(chan string, 1),
		BrokerClient: broker.NewBrokerClient(rpc),
	}
	safe.Go(func() {
		c.loop()
	})
	return c, nil
}

func (c *Client) Input(msg string) {
	c.msg <- msg
}

func (c *Client) loop() {
	for {
		select {
		case <-c.ctx.Done():
			log.FromContext(c.ctx).Debug("client receive done signal")
			return
		case msg := <-c.msg:
			if err := c.publish(msg); err != nil {
				log.FromContext(c.ctx).Errorf("publish, err: %v", err)
			}
		}
	}
}

func (c *Client) publish(msg string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := &broker.Event{Topic: c.cfg.Topic, Info: msg}
	reply, err := c.BrokerClient.Publish(ctx, event)
	if err != nil {
		return errors.WithMessage(err, "publish")
	}
	if reply.Code != 0 {
		log.FromContext(ctx).Errorf("publish err: %s", reply.Code)
	}
	return nil
}
