package cmd

import (
	"chat/im/internal/server"
	"chat/im/pkg/logger"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"strings"
)

const (
	name = "ninja"
)

var (
	cfgFile string
	rootCmd = &cobra.Command{
		Use:   "ninja",
		Short: "NinjaIM, a sleek and high-performance instant messaging platform.",
		Long:  `NinjaIM, a sleek and high-performance instant messaging platform.`,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		Run: func(cmd *cobra.Command, args []string) {
			//initServer()
		},
	}
)

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "./config.yml", "config file path")
	cobra.OnInitialize(initConfig)
}

func initConfig() {
	v := viper.New()
	if cfgFile != "" {
		// Use config file from the flag.
		v.SetConfigFile(cfgFile)
	} else {
		v.SetConfigName("config") // name of config file (without extension)
	}
	v.SetConfigType("yaml")
	//replacer := strings.NewReplacer("-", "_")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	//v.SetEnvKeyReplacer(replacer)
	v.AutomaticEnv() // read in environment variables that match

	// Add hook to set error logs to stderr and regular logs to stdout
	//logrus.AddHook(&logging.LogSplitter{})

	err := v.ReadInConfig() // Find and read the config file
	if err != nil {         // Handle errors reading the config file
		logger.Error("No valid config found: Applying default values.")
	}
	options := server.DefaultOptions()
	options.ConfigureWithViper(v)

	logger.InitWithConfig(&logger.LogOptions{
		LogLevel: options.Logger.Level,
		Name:     name,
		LogDir:   options.Logger.Dir,
	})
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
