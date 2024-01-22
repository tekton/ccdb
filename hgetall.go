package main

import (
	badger "github.com/dgraph-io/badger/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func hgetall(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Str("key", string(cmd.Args[1])).Msg("hgetall")
	return hgetallCmd(db, cmd)
}
