package main

import (
	"fmt"
	"log"
	"net"

	"github.com/schmichael/db09"
)

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)
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
		// inuse perhaps? let's try to use this as a seed!
		seeds = append(seeds, db09.NewClient(addr))
	}
	if port > 9000 {
		log.Fatalf("No unused ports below 9000.")
	}

	parts := 30
	rf := 3
	parts = parts - (parts % rf) // round down to nearest RF factor
	log.Printf("Creating db on %s with partitions= %d RF=%d seeds=%v", addr, parts, rf, seeds)
	db := db09.NewMemDB(addr, parts, rf, seeds)
	log.Printf("Serving on %s", addr)
	db09.Serve(addr, db)
	log.Println("Shutdown")
}
