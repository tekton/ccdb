package main

import (
	badger "github.com/dgraph-io/badger/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func echo(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Str("key", string(cmd.Args[1])).Msg("echo")

	var echo []byte
	for i, byteVal := range cmd.Args[1:] {
		if i > 0 {
			echo = append(echo, []byte(" ")[0])
		}
		echo = append(echo, byteVal...)
	}

	return echo, nil
}
