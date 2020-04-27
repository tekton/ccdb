package main

import (
	"fmt"
	"log"
	"strings"
	"sync"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/tidwall/redcon"
)

var addr = ":6969"

func main() {

	db, badger_err := badger.Open(badger.DefaultOptions("cc.db"))
	if badger_err != nil {
		log.Fatal(badger_err)
	}
	defer db.Close()

	// GET, SMEMBERS, HSET, HINCR, HGETALL, KEYS, SADD, SREM, DEL

	var mu sync.RWMutex
	var items = make(map[string][]byte)
	go log.Printf("started server at %s", addr)
	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "sadd":
				_key := cmd.Args[1]
				_toAdd := cmd.Args[2:]
				// for _ta in _toAdd
				log.Printf("%s", _key)
				log.Printf("%s", _toAdd)

				set_err := db.Update(func(txn *badger.Txn) error {

					for i, s := range _toAdd {
						log.Printf("%d %s", i, s)
						set_key := []byte(fmt.Sprintf("%s::%d", _key, i))
						log.Printf("setting: %s %s", set_key, s)
						err := txn.Set(set_key, s)

						if err != nil {
							log.Println(err)
							return err
						}
					}

					return nil

				})

				if set_err != nil {
					conn.WriteNull()
				} else {
					conn.WriteString("OK")
				}
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				// mu.Lock()
				// items[string(cmd.Args[1])] = cmd.Args[2]

				set_err := db.Update(func(txn *badger.Txn) error {
					err := txn.Set(cmd.Args[1], cmd.Args[2])
					return err
				})

				// mu.Unlock()
				if set_err != nil {
					conn.WriteNull()
				} else {
					conn.WriteString("OK")
				}
			case "get":
				// log.Printf("%s", cmd.Args)
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				// mu.RLock()
				// val, ok := items[string(cmd.Args[1])]

				var bdgrVal []byte
				get_err := db.View(func(txn *badger.Txn) error {
					item, g_err := txn.Get(cmd.Args[1])
					if g_err != nil {
						return g_err
					}
					err := item.Value(func(val []byte) error {
						bdgrVal = append([]byte{}, val...)
						return nil
					})

					if err != nil {
						return err
					}

					return nil
				})

				// log.Printf("%s", bdgrVal)

				// mu.RUnlock()
				if get_err != nil {
					conn.WriteNull()
				} else {
					// conn.WriteBulk(val)
					conn.WriteBulk(bdgrVal)
				}
			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				_, ok := items[string(cmd.Args[1])]
				delete(items, string(cmd.Args[1]))
				mu.Unlock()
				if !ok {
					conn.WriteInt(0)
				} else {
					conn.WriteInt(1)
				}
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
