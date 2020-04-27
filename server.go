package main

import (
	"log"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
	badger "github.com/dgraph-io/badger/v2"
)

var addr = ":6969"

func main() {

	db, badger_err := badger.Open(badger.DefaultOptions("cc.db"))
	if badger_err != nil {
		log.Fatal(badger_err)
	}
	defer db.Close()

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
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				// items[string(cmd.Args[1])] = cmd.Args[2]

				set_err := db.Update(func(txn *badger.Txn) error {
					err := txn.Set(cmd.Args[1], cmd.Args[2])
					return err
				})

				mu.Unlock()
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
				mu.RLock()
				// val, ok := items[string(cmd.Args[1])]

				
				var bdgr_val []byte
				get_err := db.View(func(txn *badger.Txn) error {
					item, g_err := txn.Get(cmd.Args[1])
					if g_err != nil {
						return g_err
					}
					err := item.Value(func(val []byte) error {
						bdgr_val = append([]byte{}, val...)
						return nil
					})

					if err != nil {
						return err
					}

					return nil
				})

				// log.Printf("%s", bdgr_val)

				mu.RUnlock()
				if get_err != nil {
					conn.WriteNull()
				} else {
					// conn.WriteBulk(val)
					conn.WriteBulk(bdgr_val)
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