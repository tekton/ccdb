package main

import (
	"bytes"
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func sadd(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Str("key", string(cmd.Args[1])).Str("members", string(bytes.Join(cmd.Args[2:], nil))).Msg("sadd")

	_key := cmd.Args[1]
	_toAdd := cmd.Args[2:]

	if err := db.Update(func(txn *badger.Txn) error {
		// set::<set_name>::member
		for _, s := range _toAdd {
			setKey := []byte(fmt.Sprintf("set::%s::%s", _key, s))
			if err := txn.Set(setKey, s); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return redcon.SimpleInt(0), nil
	}

	return redcon.SimpleInt(1), nil
}
