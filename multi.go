package main

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func multi(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Msg("multi")

	mu.Lock()
	connections[conn.RemoteAddr()].Multi = true
	mu.Unlock()
	// conn.WriteString("OK")

	return "OK", nil
}
