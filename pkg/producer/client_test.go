package producer

import (
	"context"
	"fmt"
	"testing"

	"github.com/rcy1122/msg/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	log.SetLevel(logrus.DebugLevel)
	cfg := &Config{
		Server: "127.0.0.1:17000",
		Topic:  "t1",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := New(ctx, cfg)
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		c.Input(fmt.Sprintf("%d", i))
	}
}
