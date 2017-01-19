package main

import (
	server "./server"
)

func main() {
	srv, err := server.NewServer(":55555")
	if err != nil {
		panic(err)
	}
	srv.Run()
}
