package main

import (
	"errors"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func del(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Str("key", string(cmd.Args[1])).Msg("del")

	if len(cmd.Args) != 2 {
		return nil, errors.New("ERR wrong number of arguments for 'DEL' command")
	}

	mu.Lock()
	defer mu.Unlock()

	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Delete(cmd.Args[1])
	}); err != nil {
		return redcon.SimpleInt(0), nil
	}

	return redcon.SimpleInt(1), nil
}
