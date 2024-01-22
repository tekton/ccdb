package main

import "github.com/tidwall/redcon"

type connection struct {
	Addr string

	Multi    bool
	Commands []redcon.Command
}
