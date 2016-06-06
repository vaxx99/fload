package bcd

import (
	"encoding/hex"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/vaxx99/vload/ama"
)

//Open file
func Open(fn string) (*os.File, error) {
	file, e := os.Open(fn)
	if e != nil {
		log.Println("File open error:", e)
	}
	return file, e
}

//Read file
func Read(file *os.File, bt int) ([]byte, error) {
	data := make([]byte, bt)
	_, e := file.Read(data)
	if e != nil {
		log.Println("File open error:", e)
	}
	return data, e
}

//H2i - hex to int
func H2i(dt string) int {
	ft, _ := strconv.Atoi(dt)
	return ft
}

func H2int(hexStr string) int {
	// base 16 for hexadecimal
	res, _ := strconv.ParseInt(hexStr, 16, 64)
	return int(res)
}

//H2bcd - hex to bcd
func H2bcd(dt []byte) string {
	hd := strings.ToUpper(hex.EncodeToString(dt))
	return hd
}

//Next record size
func Next(dt byte) int {
	// base 16 for hexadecimal
	res := int(dt)
	return res
}

//Finfo BCD
func Finfo(fn string) (string, string, string, int, int, *os.File, error) {
	var j int
	var mT string
	sw := ""
	ft := "NIL"
	if fn[0:2] == "am" || fn[0:2] == "rv" {
		sw = "0"
	} else {
		sw = "1"
	}

	rc := 0
	file, e := os.Open(fn)
	Err(e)
	st, e := file.Stat()
	fs := st.Size()
	Err(e)
	dt := make([]byte, 4)
	_, e = file.Read(dt)
	Err(e)
	hd := strings.ToUpper(hex.EncodeToString(dt))
	if strings.Contains(hd, "001B0000") == true {
		ft = "AMA"
		_, e = file.Seek(0, 0)
		data, e := Read(file, 31)
		Err(e)
		dt := H2bcd(data)
		mT = ama.ABor(dt)
		j = Next(data[len(data)-3])
		_, e = file.Seek(st.Size()-31, 0)
		Err(e)
		data, e = Read(file, 31)
		Err(e)
		dt = H2bcd(data)
		rc = ama.AEor(dt)
		_, e = file.Seek(31, 0)
		Err(e)
	}
	if strings.Contains(hd, "01000000") == true {
		ft = "IAD"
		_, e = file.Seek(0, 0)
		data, e := Read(file, 2048)
		Err(e)
		dt := H2bcd(data)
		mT = ama.IBor(dt)
		_, e = file.Seek(fs-2048, 0)
		Err(e)
		data, e = Read(file, 2048)
		Err(e)
		dt = H2bcd(data)
		rc = ama.IEor(dt)
		_, e = file.Seek(2048, 0)
		Err(e)
	}
	return sw, ft, mT, rc, j, file, e
}

func Date(dt string) string {
	if len(dt) == 14 {
		dt = dt[6:8] + "." + dt[4:6] + "." + dt[0:4] + " " + dt[8:10] + ":" + dt[10:12] + ":" + dt[12:14]
	} else {
		dt = ""
	}
	return dt
}

//Err - error
func Err(e error) {
	if e != nil {
		log.Println("BCD error:",e)
		return
	}
}
