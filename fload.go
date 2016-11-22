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
	"github.com/vaxx99/fload/ama"
	"github.com/vaxx99/fload/bcd"
	"github.com/vaxx99/fload/cnf"
)

type block []ama.Redrec

var cfg *cnf.Config

func opendb(path, name string, mod os.FileMode) *bolt.DB {
	db, err := bolt.Open(path+"/"+name, mod, nil)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func term(c *cnf.Config) {
	os.Mkdir(cfg.Path+"/bdb/"+cfg.Term, 0777)
	if time.Now().Format("20060102")[6:8] == "01" && time.Now().Format("150405")[0:2] == "06" {
		fmt.Println("current period:", time.Now().Format("200601"))
		s := `{"Path":"` + c.Path + `","Port":"` + c.Port + `","Term":"` + time.Now().Format("200601") + `"}`
		d := []byte(s)
		os.Mkdir(cfg.Path+"/bdb/"+time.Now().Format("200601"), 0777)
		err := ioutil.WriteFile("conf.json", d, 0644)
		check(err)
	}

}

func week(day string) string {
	var s string
	switch day {
	case "01", "02", "03", "04", "05", "06", "07":
		s = "week01"
	case "08", "09", "10", "11", "12", "13", "14":
		s = "week02"
	case "15", "16", "17", "18", "19", "20", "21":
		s = "week03"
	case "22", "23", "24", "25", "26", "27", "28", "29", "30", "31":
		s = "week04"
	}

	return s
}

func wize(db *bolt.DB) {
	t := time.Now()
	days := map[string]int{}
	bckn := map[string]string{}
	os.Chdir(cfg.Path + "/bdb/" + cfg.Term)
	f, _ := ioutil.ReadDir(".")
	for _, fn := range f {
		if fn.Name()[0:4] == "week" {
			wb := opendb(cfg.Path+"/bdb/"+cfg.Term+"/", fn.Name(), 0600)
			bn := bname(wb)
			for _, buckn := range bn {
				bckn[buckn] = fn.Name()
				wb.View(func(tx *bolt.Tx) error {
					// Assume bucket exists and has keys
					b := tx.Bucket([]byte(buckn))
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
			}
			wb.Close()
		}
	}
	db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("size"))
		for k, v := range days {
			kv := strconv.Itoa(v)
			err = bucket.Put([]byte(k), []byte(kv))
		}
		return err
	})
	db.Update(func(tx *bolt.Tx) error {
		bckt, err := tx.CreateBucketIfNotExists([]byte("buck"))
		for k, v := range bckn {
			err = bckt.Put([]byte(k), []byte(v))
		}
		return err
	})
	fmt.Printf("%5s %10d %s %8.2f\n", "size:", days["ALL"], time.Now().Format("15:04:05"), time.Now().Sub(t).Seconds())
}

func main() {
	fmt.Println("loader:", time.Now().Format("02.01.2006 15:04:05"))
	cnf.LoadConfig()
	cfg = cnf.GetConfig()
	os.Chdir(cfg.Path)
	term(cfg)
	cnf.LoadConfig()
	cfg = cnf.GetConfig()
	os.Chdir(cfg.Path + "/tmp")
	st0 := opendb(cfg.Path+"/bdb/"+cfg.Term, "stat0.db", 0666)
	defer st0.Close()
	f, _ := ioutil.ReadDir(".")
	ds := false
	for _, fn := range f {
		if fget(fn.Name(), st0) != true {
			var w1, w2, w3, w4 block
			ds = true
			t0 := time.Now()
			mn, pr, rp := rama(fn.Name())
			set("file", fn.Name(), mn[0:8], st0)
			for _, v := range rp {
				if v.Id[0:6] == cfg.Term {
					switch week(v.Id[6:8]) {
					case "week01":
						w1 = append(w1, v)
					case "week02":
						w2 = append(w2, v)
					case "week03":
						w3 = append(w3, v)
					case "week04":
						w4 = append(w4, v)
					}
				}
			}
			if len(w1) > 0 {
				wb1 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week1.db", 0666)
				rset(w1, wb1)
				wb1.Close()
			}
			if len(w2) > 0 {
				wb2 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week2.db", 0666)
				rset(w2, wb2)
				wb2.Close()
			}
			if len(w3) > 0 {
				wb3 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week3.db", 0666)
				rset(w3, wb3)
				wb3.Close()
			}
			if len(w4) > 0 {
				wb4 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week4.db", 0666)
				rset(w4, wb4)
				wb4.Close()
			}
			t1 := time.Now().Sub(t0).Seconds()
			log.Printf("%20s %10s %8d %8s %8.2f\n", fn.Name(), mn[0:8], pr, "load", t1)
		}
		os.Remove(fn.Name())
	}
	if ds {
		wize(st0)
	}
	fmt.Println("*")
}

func rama(fn string) (string, int, block) {
	var recs block
	ft, mT, rc, j, file, e := bcd.Finfo(fn)
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
				b := ama.AA(ad, yy)
				recs = append(recs, b)
			}
			if ad[0:6] == "AA9021" {
				b := ama.AA(ad, yy)
				recs = append(recs, b)
			}
			if ad[0:6] == "AA9025" {
				b := ama.AA(ad, yy)
				recs = append(recs, b)
			}
			if ad[0:6] == "AA9026" {
				b := ama.AA(ad, yy)
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
				b := ama.AA(ad, yy)
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

		for _, v := range recs {
			bucket, err := tx.CreateBucketIfNotExists([]byte(v.Id[0:8]))
			if err != nil {
				return err
			}
			key := v.Id + ".Sw." + v.Sw + ".Hi." + v.Hi + ".Na." + v.Na + ".Nb." + v.Nb + ".Ds." + v.Ds + ".De." + v.De +
				".Dr." + v.Dr + ".Ot." + v.Ot + ".It." + v.It + ".Du." + v.Du

			err = bucket.Put([]byte(key), []byte(v.Id[0:6]))

		}
		return nil
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

func bname(db *bolt.DB) []string {
	var bn []string
	db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if string(k)[0:4] != "file" && string(k)[0:4] != "size" {
				bn = append(bn, string(k))
			}
		}
		return nil
	})
	return bn
}
