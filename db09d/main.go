package main

import (
	"fmt"
	"log"
	"net"

	"github.com/schmichael/db09"
)

func main() {
	host := "localhost"
	addr := ""
	port := 2009
	seeds := []*db09.Client{} // treat any used port >= 2009 as a seed
	for ; port <= 9001; port++ {
		addr = fmt.Sprintf("%s:%d", host, port)
		s, err := net.Listen("tcp", addr)
		if err == nil {
			// found a usable port!
			s.Close()
			break
		}
		log.Println(err)
		// inuse perhaps? let's try to use this as a seed!
		seeds = append(seeds, db09.NewClient(addr))
	}
	if port > 9000 {
		log.Fatalf("No unused ports below 9000.")
	}

	log.Printf("Creating db on %s with RL=%d and seeds=%v", addr, 3, seeds)
	db := db09.NewMemDB(addr, 3, seeds)
	log.Printf("Serving on %s", addr)
	db09.Serve(addr, db)
	log.Printf("Shutdown")
}
