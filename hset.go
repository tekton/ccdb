package main

import (
	badger "github.com/dgraph-io/badger/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func hset(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Str("key", string(cmd.Args[1])).Str("field", string(cmd.Args[2])).Str("value", string(cmd.Args[3])).Msg("hset")

	keys, err := hsetCmd(db, cmd)
	if err != nil {
		return nil, err
	}

	return keys, nil
}
