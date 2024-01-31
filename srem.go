package main

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

func srem(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	if err := db.Update(func(txn *badger.Txn) error {
		for _, s := range cmd.Args[2:] {
			setKey := []byte(fmt.Sprintf("set::%s::%s", cmd.Args[1], s))
			if err := txn.Delete(setKey); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return redcon.SimpleInt(0), err
	}

	return redcon.SimpleInt(1), nil
}
