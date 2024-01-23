package main

import (
	"errors"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func set(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Str("key", string(cmd.Args[1])).Str("value", string(cmd.Args[2])).Msg("set")

	if len(cmd.Args) != 3 {
		return nil, errors.New("ERR wrong number of arguments for 'SET' command")
	}

	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(cmd.Args[1], cmd.Args[2])
	}); err != nil {
		return nil, nil
	}

	return "OK", nil
}
