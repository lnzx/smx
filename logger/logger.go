package logger

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
)

var Log zerolog.Logger

func init() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	Log = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}
