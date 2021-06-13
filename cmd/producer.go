/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rcy1122/msg/pkg/log"
	"github.com/rcy1122/msg/pkg/producer"
	"github.com/rcy1122/msg/pkg/safe"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// producerCmd represents the producer command
var producerCmd = &cobra.Command{
	Use:   "producer",
	Short: "Start a producer",
	Long:  `Use producer to publish some news`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if err := viper.Unmarshal(&cfg.Config); err != nil {
			return errors.WithMessage(err, "unmarshal config")
		}
		cfg.Printf()

		levelStr := strings.ToLower(cfg.Log.Level)
		level, err := logrus.ParseLevel(levelStr)
		if err != nil {
			level = logrus.InfoLevel
			log.WithoutContext().Infof("wrong log level, got: %s", cfg.Log.Level)
		}
		log.SetLevel(level)
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return runProducer()
	},
}

func init() {
	rootCmd.AddCommand(producerCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// producerCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// producerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	flags := producerCmd.Flags()
	flags.StringP("producer.topic", "t", "Apple", "specify topic information")
	flags.StringP("producer.server", "s", "127.0.0.1:17000", "broker service address")
	flags.StringP("log.level", "l", "info", "log level")

	if err := viper.BindPFlags(flags); err != nil {
		panic(err)
	}
}

func runProducer() error {
	ctx := contextWithSignal(context.Background())
	ctx = log.With(ctx, log.Str("msg", "producer"))

	log.FromContext(ctx).Debugf("running with topic: '%s'", cfg.Producer.Topic)
	c, err := producer.New(ctx, cfg.Producer)
	if err != nil {
		return errors.WithMessage(err, "new producer")
	}

	safe.Go(func() {
		input(ctx, c)
	})

	for {
		select {
		case <-ctx.Done():
			log.FromContext(ctx).Info("producer had stopped")
			time.Sleep(10 * time.Millisecond)
			return nil
		}
	}
}

func input(ctx context.Context, c *producer.Client) {
	var (
		in  string
		err error
	)
	inputReader := bufio.NewReader(os.Stdin)
	fmt.Printf("Please enter some input: \n")
	for {
		select {
		case <-ctx.Done():
			log.WithoutContext().Info("receive stop producer")
			return
		default:
		}
		in, err = inputReader.ReadString('\n')
		if err != nil {
			log.FromContext(ctx).Errorf("read err: %v", err)
			break
		}
		in = strings.TrimRight(in, "\n")
		c.Input(in)
	}
}
