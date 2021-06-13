package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/rcy1122/msg/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	log.SetLevel(logrus.DebugLevel)
	cfg := &Config{
		Server: "127.0.0.1:17000",
		Topic:  "t1",
		Name:   "ko",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := New(ctx, cfg)
	assert.NoError(t, err)

	err = c.Consumer()
	assert.NoError(t, err)

	c.Wait()
}
