package main

import (
	"fmt"
	"log"
	"fmt"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/spf13/viper"
)

// var addr = ":6969"
// application wide settings
var SETTINGS *viper.Viper

func init() {
	SETTINGS = viper.New()
	SETTINGS.Set("verbose", true)
	SETTINGS.AddConfigPath(".")
	SETTINGS.AddConfigPath("./config")
	SETTINGS.AddConfigPath("/etc/ccdb")
	SETTINGS.SetConfigName("ccdb")
	viper_err := SETTINGS.ReadInConfig()       // Find and read the config file
	if viper_err != nil {                      // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", viper_err))
	} else {
		fmt.Println(SETTINGS.AllKeys())
	}
}

func main() {

	db_loc := SETTINGS.GetString("badger_file")
	db, badger_err := badger.Open(badger.DefaultOptions(db_loc))
	if badger_err != nil {
		log.Fatal(badger_err)
	}
	defer db.Close()

	// GET, SMEMBERS, HSET, HINCR, HGETALL, KEYS, SADD, SREM, DEL

	var mu sync.RWMutex
	var items = make(map[string][]byte)

	addr := SETTINGS.GetString("port")

	go log.Printf("started server at %s, storring at %s", addr, db_loc)
	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			log.Printf("%s", cmd)
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("DANGER DANGER '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "hset":
				log.Printf("%s", cmd)
				keys, err := hsetCmd(db, cmd)
				if err != nil {
					conn.WriteError("Unalbe to write hash")
				} else {
					conn.WriteInt(keys)
				}
			case "hgetall":
				keys, err := hgetallCmd(db, cmd)

				if err != nil {
					conn.WriteError("ERR")
				} else {
					conn.WriteArray(len(keys) * 2)
					for k, v := range keys {
						conn.WriteBulk([]byte(k))
						conn.WriteBulk([]byte(v))
					}
				}
			case "hdel":
				conn.WriteNull()
			case "sadd":
				_key := cmd.Args[1]
				_toAdd := cmd.Args[2:]

				setErr := db.Update(func(txn *badger.Txn) error {
					// set::<set_name>::member
					for _, s := range _toAdd {
						setKey := []byte(fmt.Sprintf("set::%s::%s", _key, s))
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
				err := db.Update(func(txn *badger.Txn) error {
					for _, s := range cmd.Args[2:] {
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
						_err := _item.Value(func(_val []byte) error {
							keyz[fmt.Sprintf("%s", _val)] = fmt.Sprintf("%s", _val)
							return nil
						})
						if _err != nil {
							return _err
						}
					}

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

				setErr := db.Update(func(txn *badger.Txn) error {
					err := txn.Set(cmd.Args[1], cmd.Args[2])
					return err
				})

				if setErr != nil {
					conn.WriteNull()
				} else {
					conn.WriteString("OK")
				}
			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

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

				if getErr != nil {
					conn.WriteNull()
				} else {
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
