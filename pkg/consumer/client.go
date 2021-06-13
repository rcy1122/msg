package consumer

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rcy1122/msg/common/broker"
	"github.com/rcy1122/msg/pkg/log"
	"github.com/rcy1122/msg/pkg/safe"
	"google.golang.org/grpc"
)

type Config struct {
	Server string `json:"server" yaml:"server"`
	Topic  string `json:"topic" yaml:"topic"`
	Name   string `json:"name" yaml:"name"`
}

type Client struct {
	broker.BrokerClient

	ctx    context.Context
	cancel context.CancelFunc

	cfg  *Config
	Id   string
	stop chan struct{}
}

func New(ctx context.Context, cfg *Config) (*Client, error) {
	ctx1, cancel := context.WithCancel(ctx)
	c := &Client{
		ctx:    ctx1,
		cancel: cancel,
		cfg:    cfg,
		Id:     uuid.NewString(),
		stop:   make(chan struct{}, 0),
	}
	rpc, err := grpc.Dial(cfg.Server, grpc.WithInsecure())
	if err != nil {
		return nil, errors.WithMessage(err, "dial")
	}
	c.BrokerClient = broker.NewBrokerClient(rpc)
	return c, nil
}

func (c *Client) Consumer() error {
	safe.Go(func() {
		<-c.ctx.Done()
		c.close()
		c.stop <- struct{}{}
		close(c.stop)
	})
	defer c.cancel()

	sub := &broker.Sub{
		Topic: c.cfg.Topic,
		Name:  c.cfg.Name,
		Id:    c.Id,
	}
	stream, err := c.BrokerClient.Subscribe(c.ctx)
	if err != nil {
		return errors.WithMessagef(err, "subscribe topic: %s", c.cfg.Topic)
	}
	if err := stream.Send(sub); err != nil {
		return errors.WithMessage(err, "send sub info")
	}
	for {
		select {
		case <-c.ctx.Done():
			log.FromContext(c.ctx).Debug("client receive done signal")
			if err := stream.CloseSend(); err != nil {
				log.FromContext(c.ctx).Errorf("close send stream")
			}
			return nil
		default:
			reply, err := stream.Recv()
			if err == io.EOF {
				log.FromContext(c.ctx).Errorf("receive eof err: %s", err)
				return nil
			}
			if err != nil {
				if strings.Contains(err.Error(), "Canceled") {
					log.FromContext(c.ctx).Debugf("receive err: %s", err)
					return nil
				}
				log.FromContext(c.ctx).Errorf("receive err: %s", err)
				return nil
			}
			log.FromContext(c.ctx).Infof("received: %s", reply.Message)
		}
	}
}

func (c *Client) close() {
	unsub := &broker.Sub{
		Topic: c.cfg.Topic,
		Id:    c.Id,
		Name:  c.cfg.Name,
	}
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := c.BrokerClient.Unsubscribe(ctx, unsub)
	if err != nil {
		log.FromContext(c.ctx).Debugf("unsubscribe: %v", err)
	}
}

func (c *Client) Wait() {
	<-c.stop
}
