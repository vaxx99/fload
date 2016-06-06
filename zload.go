package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/vaxx99/zload/ama"
	"github.com/vaxx99/zload/bcd"
	"github.com/vaxx99/zload/cnf"
)

type block []ama.Redrec

var cfg *cnf.Config

func main() {
	//os.Chdir("/home/vaxx/.apps/vload")
	cnf.LoadConfig()
	cfg = cnf.GetConfig()
	os.Chdir(cfg.Path + "/tmp")
	db, err := bolt.Open(cfg.Path+"/bdb/"+time.Now().Format("200601")+".db", 0644, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	f, _ := ioutil.ReadDir(".")
	ds:=false
	for _, fn := range f {
		if fget(fn.Name(), db) != true {
			ds = true
			t0 := time.Now()
			mn, pr, rp := rama(fn.Name())
			set("file", fn.Name(), mn[0:8], db)
			rset(rp, db)
			t1 := time.Now().Sub(t0)
			log.Println(fn.Name(), mn[0:8], pr, "load", t1)
		} else {
			log.Println(fn.Name(), "skip...")
		}
		os.Remove(fn.Name())
	}
	if ds {size(db)}
}

func rama(fn string) (string, int, block) {
	var recs block
	sw, ft, mT, rc, j, file, e := bcd.Finfo(fn)
	yy := mT[0:4]
	bcd.Err(e)
	defer file.Close()
	fe := false

	i := 0

	if ft == "AMA" && fe != true {

		for {
			data, e := bcd.Read(file, j)
			j = bcd.Next(data[len(data)-3])
			bcd.Err(e)
			ad := bcd.H2bcd(data)

			if ad[0:6] == "AA9020" {
				b := ama.AA(ad, yy, sw)
				recs = append(recs, b)
			}
			if ad[0:6] == "AA9021" {
				b := ama.AA(ad, yy, sw)
				recs = append(recs, b)
			}
			if ad[0:6] == "AA9025" {
				b := ama.AA(ad, yy, sw)
				recs = append(recs, b)
			}
			if ad[0:6] == "AA9026" {
				b := ama.AA(ad, yy, sw)
				recs = append(recs, b)
			}
			if j == 0 {
				//exit on "EOF"
				return mT, rc, recs
			}
			i++
		}
	}

	if ft == "IAD" && fe != true {

		i := 0
		for i < rc {
			data, e := bcd.Read(file, 42)
			bcd.Err(e)
			ad := bcd.H2bcd(data)
			if ad[0:6] == "AA0003" {
				b := ama.AA(ad, yy, sw)
				recs = append(recs, b)
			} else {
				_, e = bcd.Read(file, 32)
				bcd.Err(e)
			}
			i++
		}
	}
	return mT, rc, recs
}

func rset(recs []ama.Redrec, db *bolt.DB) {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}

		for _, v := range recs {
			key := v.Id + ".Sw." + v.Sw + ".Hi." + v.Hi + ".Na." + v.Na + ".Nb." + v.Nb + ".Ds." + v.Ds + ".De." + v.De +
				".Dr." + v.Dr + ".Ot." + v.Ot + ".It." + v.It + ".Du." + v.Du

			err = bucket.Put([]byte(key), []byte(v.Id[0:6]))

		}
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
}

func dates(dt string) string {
	rd := ""
	if len(dt) > 0 {
		rd = dt[6:8] + "." + dt[4:6] + "." + dt[0:4] + " " + dt[8:10] + ":" + dt[10:12] + ":" + dt[12:14]
	}
	return rd
}

func rget(buck, key string, db *bolt.DB) {
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(buck))
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", buck)
		}

		val := bucket.Get([]byte(key))
		fmt.Println(string(val))
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func fget(key string, db *bolt.DB) bool {
	var f bool
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("file"))
		if bucket == nil {
			f = false
			return nil
		}

		val := bucket.Get([]byte(key))
		if val != nil {
			f = true
		} else {
			f = false
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
	return f
}

func set(buck, key, val string, db *bolt.DB) {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(buck))
		if err != nil {
			return err
		}

		err = bucket.Put([]byte(key), []byte(val))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func size(db *bolt.DB) {
	t:=time.Now()
	fmt.Println("Size Start:",t.Format("15:04:05"))
	days := map[string]int{}
	defer db.Close()
	db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte("data"))
		b.ForEach(func(k, v []byte) error {
			days["ALL"]++
			days[string(k)[0:6]]++
			days[string(k)[0:8]]++
			j := strings.Index(string(k), "Hi")
			days[string(k)[0:8]+"."+string(k)[j+3:j+7]]++
			if strings.Index(string(k), "Hi.0003") != -1 {
				//days[string(k)[0:8]+".0001"]++
			} else {
				days[string(k)[0:8]+".0001"]++
			}
			return nil
		})
		return nil
	})
	db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte("size"))
		if err != nil {
			return err
		}
		bucket, err := tx.CreateBucketIfNotExists([]byte("size"))
		for k, v := range days {
			kv := strconv.Itoa(v)
			err = bucket.Put([]byte(k), []byte(kv))
		}
		return err
	})
	fmt.Println("Size Stop :",time.Now().Format("15:04:05"),time.Now().Sub(t))
}
