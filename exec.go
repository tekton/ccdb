package main

import (
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func exec(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Msg("exec")

	mu.Lock()
	defer mu.Unlock()

	if len(connections[conn.RemoteAddr()].Commands) == 0 {
		conn.WriteNull()
		return nil, nil
	}

	responses := []any{}
	for _, c := range connections[conn.RemoteAddr()].Commands {
		cmdStr := strings.ToLower(string(c.Args[0]))
		if selectedCmd, ok := commands[cmdStr]; ok {
			res, err := selectedCmd(db, conn, c)
			if err != nil {
				return nil, err
			}
			responses = append(responses, res)
		}
	}

	connections[conn.RemoteAddr()].Multi = false
	connections[conn.RemoteAddr()].Commands = []redcon.Command{}

	return responses, nil
}
