package main

import (
	"log"
	"os"

	"github.com/hirano00o/kvstore"
)

func main() {
	d, err := kvstore.NewDal("kv.db", os.Getpagesize())
	if err != nil {
		log.Fatalf("failed to initialize data access layer: %v", err)
	}

	p := d.AllocateEmptyPage()
	p.Num = d.GetNextPage()
	copy(p.Data[:], "data")

	err = d.WritePage(p)
	if err != nil {
		log.Fatalf("failed to write page: %v", err)
	}
}
