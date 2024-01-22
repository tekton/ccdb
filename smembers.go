package main

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func smembers(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error) {
	log.Debug().Str("key", string(cmd.Args[1])).Msg("smembers")

	keyz := map[string]string{}
	err := db.View(func(txn *badger.Txn) error {
		itr := txn.NewIterator(badger.DefaultIteratorOptions)
		defer itr.Close()
		prefix := []byte(fmt.Sprintf("set::%s::", cmd.Args[1]))

		for itr.Seek(prefix); itr.ValidForPrefix(prefix); itr.Next() {
			_item := itr.Item()
			_err := _item.Value(func(_val []byte) error {
				keyz[string(_val)] = string(_val)
				return nil
			})
			if _err != nil {
				return _err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	} else {
		conn.WriteArray(len(keyz))
		for _, v := range keyz {
			conn.WriteBulk([]byte(v))
		}
	}

	return keyz, nil
}
