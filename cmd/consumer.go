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
	"context"
	"strings"

	"github.com/pkg/errors"
	"github.com/rcy1122/msg/pkg/consumer"
	"github.com/rcy1122/msg/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// consumerCmd represents the consumer command
var consumerCmd = &cobra.Command{
	Use:   "consumer",
	Short: "Start a consumer",
	Long:  `Use consumer to subscribe some news`,
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
		return runConsumer()
	},
}

func init() {
	rootCmd.AddCommand(consumerCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// consumerCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// consumerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	flags := consumerCmd.Flags()
	flags.StringP("consumer.server", "s", "127.0.0.1:17000", "broker service address")
	flags.StringP("consumer.topic", "t", "Apple", "specify topic information")
	flags.StringP("consumer.name", "n", "tom", "consumer name")
	if err := viper.BindPFlags(flags); err != nil {
		panic(err)
	}
}

func runConsumer() error {
	ctx := contextWithSignal(context.Background())
	ctx = log.With(ctx, log.Str("msg", "consumer"))

	log.FromContext(ctx).Infof("I'm %s, consumer with topic: '%s'",cfg.Consumer.Name, cfg.Consumer.Topic)
	c, err := consumer.New(ctx, cfg.Consumer)
	if err != nil {
		return errors.WithMessage(err, "new consumer")
	}
	if err := c.Consumer(); err != nil {
		return errors.WithMessage(err, "consumer")
	}

	c.Wait()
	log.FromContext(ctx).Infof("consumer has stopped")
	return nil
}
