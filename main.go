package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"

	badger "github.com/dgraph-io/badger/v2"
)

// const licFile = "fccdata/fcc_lic_vw.csv"

const licFile = "fcc_8M.csv"

func main() {
	opts := badger.DefaultOptions("test.db")
	//	opts.MaxTableSize = 1 << 20
	//	opts.MaxTableSize = 40960000 (good at 1.5gb)
	//	opts.MaxTableSize = 81920000 (OOMs at 1.5gb)
	// opts.MaxTableSize = 20480000 (better at 1.5gb)
	//	opts.MaxTableSize = 10240000 (better still)
	// opts.MaxTableSize = 5120000 (better, adding a few seconds each 1/2)
	// opts.MaxTableSize = 2560000 OOMS
	opts.MaxTableSize = 7680000 // all 19m records, under 1.5gb ram

	opts.WithNumMemtables(1)
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2
	opts.SyncWrites = false
	opts.TableLoadingMode = 0
	opts.ValueLogLoadingMode = 0
	db, err := badger.Open(opts)
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

		err = txn.Set([]byte(vals[0]), []byte(line))
		if err != nil {
			log.Fatalf("txn.Set: %v", err)
		}
		writeCount++

		if writeCount%10000 == 0 {
			err = txn.Commit()
			if err != nil {
				log.Fatalf("at %d txn.Commit():%v", writeCount, err)
			}
			txn = db.NewTransaction(true)
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Fatalf("txn.Commit(): %v", err)
	}
	log.Printf("txn.Commit() final at %d", writeCount)

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
