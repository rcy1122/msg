package config

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/rcy1122/msg/pkg/broker"
	"github.com/rcy1122/msg/pkg/consumer"
	"github.com/rcy1122/msg/pkg/log"
	"github.com/rcy1122/msg/pkg/producer"
)

type MsgConfig struct {
	Config `export:"true"`
}

type Config struct {
	Broker   *broker.Config   `json:"broker" yaml:"broker"`
	Producer *producer.Config `json:"producer" yaml:"producer"`
	Consumer *consumer.Config `json:"consumer" yaml:"consumer"`
	Log      Log              `json:"log" yaml:"log"`
}

type Log struct {
	Level string `json:"level" yaml:"level"`
}

func (mc *Config) Printf() {
	b, err := mc.String()
	if err != nil {
		log.WithoutContext().Errorf("marshal conf: %+v", err)
	}
	log.WithoutContext().Printf("config: %s", b)
}

func (mc *Config) String() (string, error) {
	b, err := json.MarshalIndent(mc, "", "  ")
	if err != nil {
		return "", errors.WithMessage(err, "marshal bcs config")
	}
	return string(b), nil
}
