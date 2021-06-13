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
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pkg/errors"
	"github.com/rcy1122/msg/pkg/broker"
	"github.com/rcy1122/msg/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// brokerCmd represents the broker command
var brokerCmd = &cobra.Command{
	Use:   "broker",
	Short: "Start a broker. ",
	Long:  `Open a broker on port 17000 by default`,
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
		return runBroker()
	},
}

func init() {
	rootCmd.AddCommand(brokerCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// brokerCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// brokerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	flags := brokerCmd.Flags()
	flags.StringP("broker.address", "a", "127.0.0.1:17000", "broker listen address")
	flags.StringP("log.level", "l", "info", "log level")
	if err := viper.BindPFlags(flags); err != nil {
		panic(err)
	}

}

func runBroker() error {
	ctx := contextWithSignal(context.Background())
	ctx = log.With(ctx, log.Str("msg", "broker"))

	s, err := broker.New(ctx)
	if err != nil {
		return errors.WithMessage(err, "new")
	}
	if err := s.Start(cfg.Broker); err != nil {
		return errors.WithMessage(err, "start")
	}
	log.FromContext(ctx).Debug("broker end")
	return nil
}

func contextWithSignal(ctx context.Context) context.Context {
	newCtx, cancel := context.WithCancel(ctx)
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		cancel()
	}()
	return newCtx
}
