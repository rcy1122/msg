package broker

import (
	"context"
	"reflect"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/rcy1122/msg/common/broker"
	"github.com/rcy1122/msg/pkg/log"
	"github.com/rcy1122/msg/pkg/safe"
)

type eventHandler struct {
	callBack   reflect.Value
	subscriber string
}

type subscriber struct {
	id   string
	name string
	sub  broker.Broker_SubscribeServer
}

type Broker struct {
	broker.UnimplementedBrokerServer

	ctx     context.Context
	transit chan *broker.Event

	lock     sync.RWMutex
	handlers map[string][]*subscriber // topic:user:stream
}

func newBroker(ctx context.Context) *Broker {
	b := &Broker{
		ctx:      ctx,
		transit:  make(chan *broker.Event, 1000),
		lock:     sync.RWMutex{},
		handlers: make(map[string][]*subscriber),
	}
	safe.Go(func() {
		b.loop(ctx)
	})
	return b
}

func (b *Broker) Publish(ctx context.Context, in *broker.Event) (*broker.Reply, error) {
	ctx = log.With(ctx, log.Str("msg", "broker"))
	log.FromContext(ctx).Debugf("receive: %v", in)

	b.transit <- in

	reply := &broker.Reply{
		Code:    0,
		Message: in.Info,
	}

	return reply, nil
}

func (b *Broker) Subscribe(stream broker.Broker_SubscribeServer) error {
	in, err := stream.Recv()
	if err != nil {
		return errors.WithMessage(err, "receive subscribe")
	}
	log.FromContext(b.ctx).Infof("'%s' started subscribing to '%s'", in.Name, in.Topic)
	if err := b.doSubscribe(in.Topic, in.Id, in.Name, stream); err != nil {
		return errors.WithMessage(err, "subscribe")
	}
	for {
		select {
		case <-b.ctx.Done():
			_ = stream.Send(&broker.Reply{Code: 500, Message: "service is closed"})
			return nil
		}
	}
}

func (b *Broker) Unsubscribe(ctx context.Context, in *broker.Sub) (*broker.Empty, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	log.FromContext(ctx).Infof("'%s' has unsubscribed from '%s'", in.Name, in.Topic)
	idx := -1
	if handlers, ok := b.handlers[in.Topic]; ok && len(handlers) > 0 {
		for i, v := range handlers {
			if v.id == in.Id {
				idx = i
				break
			}
		}
	}
	if idx == -1 {
		return &broker.Empty{}, nil
	}
	l := len(b.handlers[in.Topic])
	copy(b.handlers[in.Topic][idx:], b.handlers[in.Topic][idx+1:])
	b.handlers[in.Topic][l-1] = nil // or the zero value of T
	b.handlers[in.Topic] = b.handlers[in.Topic][:l-1]

	return &broker.Empty{}, nil
}

func (b *Broker) doSubscribe(topic, subscriberId, subscriberName string, handler broker.Broker_SubscribeServer) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	ser := &subscriber{
		id:   subscriberId,
		name: subscriberName,
		sub:  handler,
	}
	b.handlers[topic] = append(b.handlers[topic], ser)
	return nil
}

func (b *Broker) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.FromContext(ctx).Debug("broker receive done signal. ")
			return
		case msg := <-b.transit:
			log.FromContext(ctx).Infof("publish: %v", msg)
			b.lock.Lock()
			if handlers, ok := b.handlers[msg.Topic]; ok && len(handlers) > 0 {
				copyHandlers := make([]*subscriber, len(handlers))
				copy(copyHandlers, handlers)
				for _, v := range copyHandlers {
					tmp := v
					reply := &broker.Reply{
						Code:    0,
						Message: msg.Info,
					}
					if err := tmp.sub.Send(reply); err != nil {
						if !strings.Contains(err.Error(), "transport is closing") {
							log.FromContext(ctx).Debugf("publish err: %v ", err)
						}
					}
				}
				b.lock.Unlock()
				break
			}
			log.FromContext(ctx).Debugf("discard, %v", msg)
			b.lock.Unlock()
		}
	}
}
