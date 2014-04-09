/*
 * Copyright (c) 2014 Kurt Jung (Gmail: kurt.w.jung)
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package qlm_test

import (
	"code.google.com/p/qlm"
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"math/big"
	"time"
)

// Hello, ql/m
func ExampleDbType_01() {
	var db *qlm.DbType
	fmt.Printf("Hello, %s\n", db)
	// Output:
	// Hello, ql/m
}

// This example demonstrates a simple use of qlm. Note the use of Go slice and
// comparison expressions in the WHERE clause. Also note that replacement
// parameters use a one-based index to access parameters that follow the clause
// in the call to Retrieve().
func ExampleDbType_02() {
	type recType struct {
		ID   int64  `ql_table:"rec"`
		Name string `ql:"*"`
	}
	db := qlm.DbCreate("data/example.ql")
	db.TableCreate(&recType{})
	db.Insert([]recType{{0, "Athos"}, {0, "Porthos"}, {0, "Aramis"}})
	var list []recType
	db.Retrieve(&list, "WHERE Name[0:1] == ?1", "A")
	for _, r := range list {
		fmt.Println(r.Name)
	}
	db.Close()
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// Aramis
	// Athos
}

// This example demonstrates the use of blobs in qlm.
func ExampleDbType_03() {
	type recType struct {
		ID      int64  `ql_table:"image"`
		Img     []byte `ql:"*"`
		FileStr string `ql:"*"`
	}
	var rec recType
	var err error
	rec.FileStr = "qlm.jpg"
	rec.Img, err = ioutil.ReadFile(rec.FileStr)
	if err == nil {
		chksum := sha1.Sum(rec.Img)
		db := qlm.DbCreate("data/example.ql")
		db.TableCreate(&rec)
		db.Insert([]recType{rec})
		var list []recType
		db.Retrieve(&list, "WHERE FileStr == ?1", rec.FileStr)
		if len(list) == 1 {
			if chksum == sha1.Sum(list[0].Img) {
				fmt.Printf("%s, SHA1: %v, length: %d\n",
					rec.FileStr, chksum, len(rec.Img))
			}
		}
		db.Close()
		err = db.Error()
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// qlm.jpg, SHA1: [134 36 185 180 116 197 118 243 84 169 19 135 182 155 151 51 251 84 111 175], length: 2685
}

// This example demonstrates the use of time, bigint and rational fields. These
// are non-native types that are defined in libraries.
func ExampleDbType_04() {
	type recType struct {
		ID    int64         `ql_table:"lib"`
		Tm    time.Time     `ql:"tm"`
		Dur   time.Duration `ql:"dur"`
		Ratio big.Rat       `ql:"ratio"`
		Amt   big.Int       `ql:"amt"`
	}
	db := qlm.DbCreate("data/example.ql")
	var list []recType
	var rec recType
	db.TableCreate(&rec)
	rec.Tm = time.Date(1927, 9, 20, 12, 0, 0, 0, time.UTC)
	rec.Dur, _ = time.ParseDuration("168h")
	var j, num, den int64
	num = 52
	den = 53
	for j = 0; j < 3; j++ {
		num++
		den++
		rec.Tm = rec.Tm.Add(rec.Dur)
		rec.Ratio.SetFrac64(num, den)
		rec.Amt.SetInt64(num*den + j)
		list = append(list, rec)
	}
	db.Insert(list)
	list = nil
	db.Retrieve(&list, "ORDER BY id()")
	for _, r := range list {
		fmt.Printf("%s %s %s %s\n", r.Tm, r.Dur, &r.Ratio, &r.Amt)
	}
	db.Close()
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// 1927-09-27 12:00:00 +0000 UTC 168h0m0s 55/56 3082
	// 1927-10-04 12:00:00 +0000 UTC 168h0m0s 55/56 3082
	// 1927-10-11 12:00:00 +0000 UTC 168h0m0s 55/56 3082
}
