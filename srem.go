package main

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
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
		return nil, err
	}

	return "OK", nil
}
