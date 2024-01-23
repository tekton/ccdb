package main

import (
	"fmt"
	"strings"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/tidwall/redcon"
)

// var addr = ":6969"
// application wide settings
var SETTINGS *viper.Viper

var commands = map[string]func(db *badger.DB, conn redcon.Conn, cmd redcon.Command) (any, error){
	"ping":     ping,
	"set":      set,
	"get":      get,
	"del":      del,
	"keys":     keys,
	"sadd":     sadd,
	"srem":     srem,
	"smembers": smembers,
	"hset":     hset,
	"hget":     hget,
	"hdel":     hdel,
	"hgetall":  hgetall,
	"multi":    multi,
	"echo":     echo,
	"quit":     quit,
}

var mu sync.RWMutex
var connections = map[string]*connection{}

func init() {
	SETTINGS = viper.New()
	SETTINGS.Set("verbose", true)
	SETTINGS.AddConfigPath(".")
	SETTINGS.AddConfigPath("./config")
	SETTINGS.AddConfigPath("/etc/ccdb")
	SETTINGS.SetConfigName("ccdb")

	// Find and read the config file
	if err := SETTINGS.ReadInConfig(); err != nil {
		panic(fmt.Errorf("Error reading config file: %s \n", err))
	}

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if SETTINGS.GetString("log_level") == "debug" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	dbLoc := SETTINGS.GetString("badger_file")
	db, err := badger.Open(badger.DefaultOptions(dbLoc))
	if err != nil {
		log.Fatal().Err(err).Msg("Badger Error")
	}
	defer db.Close()

	// GET, SMEMBERS, HSET, HINCR, HGETALL, KEYS, SADD, SREM, DEL

	addr := SETTINGS.GetString("port")

	log.Info().Str("Port", addr).Str("DB", dbLoc).Msg("ccdb started")
	if err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			log.Debug().Str("cmd", fmt.Sprintf("%s", cmd.Args[0])).Str("value", fmt.Sprintf("%s", cmd.Args[1:])).Msg("query")

			cmdStr := strings.ToLower(string(cmd.Args[0]))

			// initial connection
			if cmdStr == "command" {
				conn.WriteString("OK")
				return
			}

			// handle executing multi
			// exec has to be handled here to
			// prevent a deadlock
			if cmdStr == "exec" {
				res, err := exec(db, conn, cmd)
				if err != nil {
					conn.WriteError(err.Error())
					return
				}

				writeResponse(conn, res)
				return
			}

			// make sure we support the command
			if selectedCmd, ok := commands[cmdStr]; ok {
				// handle multi
				if connections[conn.RemoteAddr()].Multi {
					mu.Lock()
					defer mu.Unlock()

					connections[conn.RemoteAddr()].Commands = append(connections[conn.RemoteAddr()].Commands, cmd)
					conn.WriteString("QUEUED")
					return
				}

				// handle commands
				res, err := selectedCmd(db, conn, cmd)
				if err != nil {
					conn.WriteError(err.Error())
					return
				}

				writeResponse(conn, res)
				return
			}

			// unknown command
			conn.WriteError("ERR unknown command '" + cmdStr + "'")
		},
		func(conn redcon.Conn) bool {
			log.Debug().Str("addr", conn.RemoteAddr()).Msg("connection established")
			connections[conn.RemoteAddr()] = &connection{
				Addr:     conn.RemoteAddr(),
				Multi:    false,
				Commands: []redcon.Command{},
			}
			return true
		},
		func(conn redcon.Conn, err error) {
			log.Debug().Err(err).Str("addr", conn.RemoteAddr()).Msg("connection closed") //("closed: %s, err: %v", conn.RemoteAddr(), err)
			connections[conn.RemoteAddr()] = nil
		},
	); err != nil {
		log.Fatal().Err(err)
	}
}

func writeResponse(conn redcon.Conn, res any) {
	log.Debug().Str("response", fmt.Sprintf("%v", res)).Msg("response")
	switch res := res.(type) {
	case string:
		conn.WriteString(res)
	case []byte:
		conn.WriteBulk(res)
	case int:
		conn.WriteInt(res)
	case redcon.SimpleInt:
		conn.WriteInt(int(res))
	case []string:
		conn.WriteArray(len(res))
		for _, v := range res {
			conn.WriteString(v)
		}
	case []any:
		conn.WriteArray(len(res))
		for _, v := range res {
			writeResponse(conn, v)
		}
	case map[string]any:
		conn.WriteArray(len(res))
		for _, v := range res {
			conn.WriteBulk([]byte(v.(string)))
		}
	case nil:
		conn.WriteNull()
	default:
		conn.WriteString(fmt.Sprintf("%v", res))
	}
}
