package main

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/tidwall/redcon"
)

func hsetCmd(db *badger.DB, cmd redcon.Command) (int, error) {
	_key := cmd.Args[1]
	_toAdd := cmd.Args[2:]
	j := 0

	setErr := db.Update(func(txn *badger.Txn) error {
		// set::<set_name>::member

		for i := 0; i < len(_toAdd); i += 2 {
			setKey := []byte(fmt.Sprintf("hash::%s::%s", _key, _toAdd[i]))
			_v := i + 1

			err := txn.Set(setKey, _toAdd[_v])

			if err != nil {
				return err
			} else {
				j++
			}
		}

		return nil
	})

	return j, setErr
}

func hgetallCmd(db *badger.DB, cmd redcon.Command) (map[string]string, error) {
	keyz := map[string]string{}

	err := db.View(func(txn *badger.Txn) error {
		itr := txn.NewIterator(badger.DefaultIteratorOptions)
		defer itr.Close()
		prefix := []byte(fmt.Sprintf("hash::%s::", cmd.Args[1]))

		for itr.Seek(prefix); itr.ValidForPrefix(prefix); itr.Next() {
			_item := itr.Item()
			_key := _item.Key()[len(prefix):]
			_err := _item.Value(func(_val []byte) error {
				keyz[fmt.Sprintf("%s", _key)] = fmt.Sprintf("%s", _val)
				return nil
			})
			if _err != nil {
				return _err
			}
		}

		return nil
	})

	return keyz, err
}

func hgetCmd(db *badger.DB, cmd redcon.Command) ([]byte, error) {
	var bdgrVal []byte
	_key := cmd.Args[1]
	_toGet := cmd.Args[2]
	getKey := []byte(fmt.Sprintf("hash::%s::%s", _key, _toGet))
	getErr := db.View(func(txn *badger.Txn) error {
		item, gErr := txn.Get(getKey)
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

	return bdgrVal, getErr
}
