package main

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func quit(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Msg("quit")

	defer conn.Close()
	return "OK", nil
}
