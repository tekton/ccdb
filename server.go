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

	db, badger_err := badger.Open(badger.DefaultOptions("_db/cc.db"))
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
				// log.Printf("%s", _key)
				// log.Printf("%s", _toAdd)

				setErr := db.Update(func(txn *badger.Txn) error {
					// set::<set_name>::member
					for _, s := range _toAdd {
						// log.Printf("%d %s", i, s)
						setKey := []byte(fmt.Sprintf("set::%s::%s", _key, s))
						// log.Printf("setting: %s %s", setKey, s)
						err := txn.Set(setKey, s)

						if err != nil {
							log.Println(err)
							return err
						}
					}
					return nil
				})

				if setErr != nil {
					conn.WriteNull()
				} else {
					conn.WriteString("OK")
				}
			case "srem":
				// log.Printf("%s %s", cmd.Args[1], cmd.Args[2:])
				err := db.Update(func(txn *badger.Txn) error {
					// log.Printf("range %d", len(cmd.Args[2:]))
					for _, s := range cmd.Args[2:] {
						// log.Printf("%d %s", i, s)
						setKey := []byte(fmt.Sprintf("set::%s::%s", cmd.Args[1], s))
						dErr := txn.Delete(setKey)
						if dErr != nil {
							log.Printf("dErr: %s", dErr)
							return dErr
						}
					}

					return nil
				})

				if err != nil {
					conn.WriteError("Unable to delete")
				} else {
					conn.WriteString("OK")
				}
			case "smembers":
				// log.Printf("%s", cmd)
				keyz := map[string]string{}
				err := db.View(func(txn *badger.Txn) error {
					itr := txn.NewIterator(badger.DefaultIteratorOptions)
					defer itr.Close()
					prefix := []byte(fmt.Sprintf("set::%s::", cmd.Args[1]))

					for itr.Seek(prefix); itr.ValidForPrefix(prefix); itr.Next() {
						_item := itr.Item()
						// _key := _item.Key()
						_err := _item.Value(func(_val []byte) error {
							// log.Printf("%s :: %s", _key, _val)
							keyz[fmt.Sprintf("%s", _val)] = fmt.Sprintf("%s", _val)
							return nil
						})
						if _err != nil {
							return _err
						}
					}

					// log.Printf("Length: %d", len(keyz))
					// log.Printf("%s", keyz)

					return nil
				})

				if err != nil {
					conn.WriteError("ERR")
				} else {
					conn.WriteArray(len(keyz))
					for _, v := range keyz {
						conn.WriteBulk([]byte(v))
					}
				}
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				// mu.Lock()
				// items[string(cmd.Args[1])] = cmd.Args[2]

				setErr := db.Update(func(txn *badger.Txn) error {
					err := txn.Set(cmd.Args[1], cmd.Args[2])
					return err
				})

				// mu.Unlock()
				if setErr != nil {
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
				getErr := db.View(func(txn *badger.Txn) error {
					item, gErr := txn.Get(cmd.Args[1])
					if gErr != nil {
						return gErr
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
				if getErr != nil {
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
