package main

import (
	"bytes"
	"errors"
	"slices"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func keys(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Str("pattern", string(cmd.Args[1])).Msg("keys")

	if len(cmd.Args) != 2 {
		return nil, errors.New("ERR wrong number of arguments for 'KEYS' command")
	}

	keys := []string{}
	if err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		if string(cmd.Args[1]) != "*" {
			opts.Prefix = []byte(cmd.Args[1])
		}

		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if bytes.Contains(it.Item().Key(), []byte("set::")) {
				continue
			}
			k := string(it.Item().Key())
			if slices.Contains(keys, k) {
				continue
			}
			keys = append(keys, k)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	return keys, nil
}
