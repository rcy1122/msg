package broker

import (
	"context"
	"net"

	"github.com/pkg/errors"
	"github.com/rcy1122/msg/common/broker"
	"github.com/rcy1122/msg/pkg/log"
	"github.com/rcy1122/msg/pkg/safe"
	"google.golang.org/grpc"
)

type Config struct {
	Address string `json:"address" yaml:"address"`
}

type Server struct {
	ctx  context.Context
	grpc *grpc.Server
}

func New(ctx context.Context) (*Server, error) {
	grpcServer := grpc.NewServer()
	broker.RegisterBrokerServer(grpcServer, newBroker(ctx))
	return &Server{grpc: grpcServer, ctx: ctx}, nil
}

func (s *Server) Start(cfg *Config) error {
	safe.Go(func() {
		<-s.ctx.Done()
		log.FromContext(s.ctx).Infof("serve stopping")
		s.grpc.Stop()
	})
	lis, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return errors.WithMessage(err, "listen")
	}

	log.FromContext(s.ctx).Infof("serve started")
	if err := s.grpc.Serve(lis); err != nil {
		return errors.WithMessage(err, "serve")
	}
	return nil
}
