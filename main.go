package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
)

const licFile = "fcc_lic_vw.csv"

func main() {
	db, err := badger.Open(badger.DefaultOptions("test.db"))
	if err != nil {
		log.Fatalf("badger.Open: %v", err)
	}
	defer db.Close()

	sf, err := os.Open(licFile)
	if err != nil {
		log.Fatalf("sf os.Open: %v", err)
	}
	defer sf.Close()

	cf, err := os.Open(licFile)
	if err != nil {
		log.Fatal("cf os.Open: %v", err)
	}

	txn := db.NewTransaction(true)
	defer txn.Discard()

	reader := csv.NewReader(cf)
	_, _ = reader.Read() // discard header

	scanner := bufio.NewScanner(sf)
	scanner.Scan() // throw away header

	writeCount := 0
	for {
		scanner.Scan()
		vals, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("reader.Read(): %v", err)
		}
		line := scanner.Text()

		if len(vals) < 3 {
			log.Fatal("csv []string is $d elements long", len(vals))
		}

		//log.Println(line)
		err = txn.Set([]byte(vals[0]), []byte(line))
		if err != nil {
			log.Fatalf("txn.Set: %v", err)
		}
		writeCount++

		if writeCount%100 == 0 {
			err = txn.Commit()
			if err != nil {
				log.Fatalf("at %d txn.Commit():%v", writeCount, err)
			}
			// log.Printf("txn.Commit() at %d", writeCount)
			txn = db.NewTransaction(true)
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Fatalf("txn.Commit(): %v", err)
	}
	log.Printf("txn.Commit() final at %d", writeCount)

	txn = db.NewTransaction(true)
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err = item.Value(func(val []byte) error {
				// you'd never do this, I want to see that iteration is really happening
				if strings.Contains(string(val), "KJ6CBE") {
					log.Print(string(val))
				}
				return nil
			})
			if err != nil {
				log.Fatalf("iterate item.Value(): %v", err)
			}
		}
		return nil
	})

	txn = db.NewTransaction(false)
	item, err := txn.Get([]byte("3130538"))
	if err != nil {
		fmt.Errorf("txn.Get(): %v", err)
	}
	err = item.Value(func(val []byte) error {
		fmt.Println(string(val))
		return nil
	})
	if err != nil {
		log.Fatalf("item.Value: %v", err)
	}
}
