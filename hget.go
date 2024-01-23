package main

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func hget(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Str("key", string(cmd.Args[1])).Str("field", string(cmd.Args[2])).Msg("hget")

	bdgrVal, getErr := hgetCmd(db, cmd)
	if getErr != nil {
		conn.WriteNull()
		return nil, getErr
	}

	return bdgrVal, nil
}
