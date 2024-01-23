package main

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

func ping(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	return "PONG", nil
}
