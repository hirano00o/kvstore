package main

import (
	"log"

	"github.com/hirano00o/kvstore"
)

func main() {
	// Initialize database
	d, err := kvstore.NewDal("kv.db")
	if err != nil {
		log.Fatalf("failed to initialize data access layer: %v", err)
	}

	p := d.AllocateEmptyPage()
	p.Num = d.GetNextPage()
	copy(p.Data[:], "data")

	// Commit
	err = d.WritePage(p)
	if err != nil {
		log.Fatalf("failed to write page: %v", err)
	}
	_, err = d.WriteFreeList()
	if err != nil {
		log.Fatalf("failed to free list: %v", err)
	}

	err = d.Close()
	if err != nil {
		log.Fatalf("failed to close: %v", err)
	}

	// Reopen database
	d, err = kvstore.NewDal("kv.db")
	if err != nil {
		log.Fatalf("failed to initialize data access layer: %v", err)
	}

	p = d.AllocateEmptyPage()
	p.Num = d.GetNextPage()
	copy(p.Data[:], "data2")

	err = d.WritePage(p)
	if err != nil {
		log.Fatalf("failed to write page: %v", err)
	}
	n := d.GetNextPage()
	d.ReleasePage(n)

	// Commit
	_, err = d.WriteFreeList()
	if err != nil {
		log.Fatalf("failed to free list: %v", err)
	}
}
