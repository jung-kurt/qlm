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
	"github.com/cznic/ql"
	"io/ioutil"
	"math/big"
	"os"
	"time"
)

// This example demonstrates a simple use of qlm. Note the use of Go slice and
// comparison expressions in the WHERE clause. Also note that replacement
// parameters use a one-based index to access parameters that follow the clause
// in the call to Retrieve().
func ExampleDbType_01() {
	type recType struct {
		ID   int64  `ql_table:"rec"`
		Name string `ql:"*"`
	}
	db := qlm.DbCreate("data/example.ql")
	db.TableCreate(&recType{})
	db.Insert([]recType{{0, "Athos"}, {0, "Porthos"}, {0, "Aramis"}})
	var list []recType
	db.Retrieve(&list, "WHERE Name[0:1] == ?1", "A")
	fmt.Println(db)
	for _, r := range list {
		fmt.Println(r.Name)
	}
	db.Close()
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// ql/m
	// Aramis
	// Athos
}

// This example demonstrates the use of blobs in qlm.
func ExampleDbType_02() {
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
// are non-native types that are defined in libraries. Caveat: big.Int and
// big.Rat values are, like slices, references. Calling Set or friends on them
// modifies the referenced components. Consequently, when building up a slice
// to pass to Insert, be sure to use distinct instances of these types.
//
// The ORDER clause in this example uses id(). This references the int64 unique
// ID that is generated automatically by ql for each record in a table. See the
// ql documentation for more details on expressions.
func ExampleDbType_03() {
	type recType struct {
		ID    int64         `ql_table:"lib"`
		Tm    time.Time     `ql:"*"`
		Dur   time.Duration `ql:"*"`
		Ratio big.Rat       `ql:"*"`
		Amt   big.Int       `ql:"*"`
	}
	db := qlm.DbCreate("data/example.ql")
	var rl [3]recType
	db.TableCreate(&recType{})
	tm := time.Date(1927, 9, 20, 12, 0, 0, 0, time.UTC)
	for j := range rl {
		rl[j].Dur, _ = time.ParseDuration(fmt.Sprintf("%dh", 168*j+5))
		rl[j].Tm = tm.Add(rl[j].Dur)
		rl[j].Ratio.SetFrac64(int64(52+j), int64(53+j))
		rl[j].Amt.SetInt64(int64(j*1045 + j + 1))
	}
	db.Insert(rl[:])
	var list []recType
	db.Retrieve(&list, "ORDER BY id()")
	for _, r := range list {
		fmt.Printf("%s %12s %8s %8s\n", r.Tm, r.Dur, r.Ratio.String(), r.Amt.String())
	}
	db.Close()
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// 1927-09-20 17:00:00 +0000 UTC       5h0m0s    52/53        1
	// 1927-09-27 17:00:00 +0000 UTC     173h0m0s    53/54     1047
	// 1927-10-04 17:00:00 +0000 UTC     341h0m0s    54/55     2093
}

// This example demonstrates the reopening of a ql database.
//
// This example shows the optional provision of names in the "ql" field tags.
// The tag values are used as field names by the ql database. This feature is
// generally useful only if the ql database is used by multiple applications.
// It is these names rather than the names in the Go structure that are used in
// expressions passed to Retrieve() and for limiting fields to be updated in
// Update().
//
// Notice that the type of the expressions for the ?1 and ?2 parameters need to
// match the type of the group_num field (in this case int64).
//
// The Retrieve function appends selection results to the passed-in slice. If
// you wish to repopulate the slice, empty it prior to calling Retrieve by
// assigning nil to it.
func ExampleDbType_04() {
	dbFileStr := "data/example.ql"
	type recType struct {
		ID   int64  `ql_table:"rec"`
		Name string `ql:"last_name"`
		Num  int64  `ql:"group_num"`
		Val  int    // exported but not managed by ql
		val  int    // not exported
	}
	db := qlm.DbCreate(dbFileStr)
	db.TableCreate(&recType{})
	var list []recType
	for j := 0; j < 1024; j++ {
		list = append(list, recType{0, fmt.Sprintf("*** %4d ***", j),
			int64(j), j * 2, j * 4})
	}
	db.Insert(list)
	db.Close()
	if db.OK() {
		db := qlm.DbOpen(dbFileStr)
		list = nil // Reuse the slice but empty it first
		db.Retrieve(&list, "WHERE group_num > ?1 && group_num < ?2 ORDER BY group_num",
			int64(1000), int64(1004))
		for _, r := range list {
			fmt.Printf("%s %d %d %d\n", r.Name, r.Num, r.Val, r.val)
		}
		db.Close()
	}
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// *** 1001 *** 1001 0 0
	// *** 1002 *** 1002 0 0
	// *** 1003 *** 1003 0 0
}

// This example demonstrates a record update. In the first call to Update(),
// only fields B and C are updated. In the second call, all fields are updated.
func ExampleDbType_05() {
	type recType struct {
		ID      int64 `ql_table:"rec"`
		A, B, C int64 `ql:"*"`
	}
	var rec recType
	db := qlm.DbCreate("data/example.ql")
	db.TableCreate(&recType{})
	adjust := func() {
		rec.A += 1000
		rec.B += 1000
		rec.C += 1000
	}
	retrieve := func() {
		var rl []recType
		// fmt.Printf("Rec ID %d\n", rec.ID)
		db.Retrieve(&rl, "WHERE id() == ?1", rec.ID)
		if len(rl) > 0 {
			for _, r := range rl {
				fmt.Printf("%d %d %d\n", r.A, r.B, r.C)
			}

		}
	}
	var list []recType
	for j := int64(0); j < 10; j++ {
		list = append(list, recType{0, j, j + 1, j + 2})
	}
	db.Insert(list)
	list = nil
	db.Retrieve(&list, "WHERE A == ?1", int64(2))
	if len(list) > 0 {
		rec = list[0]
		adjust()
		db.Update(&rec, "B", "C") // Update only B and C in the database
		retrieve()
		adjust()
		db.Update(&rec, "*") // Update all fields
		retrieve()
	}
	db.Close()
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// 2 1003 1004
	// 2002 2003 2004
}

// This example demonstrates record deletion.
func ExampleDbType_06() {
	type recType struct {
		ID      int64 `ql_table:"rec"`
		A, B, C int64 `ql:"*"`
	}
	db := qlm.DbCreate("data/example.ql")
	show := func(str string) {
		var rs []recType
		db.Retrieve(&rs, "ORDER BY A")
		fmt.Printf("%s\n", str)
		for _, r := range rs {
			fmt.Printf("%d %d %d\n", r.A, r.B, r.C)
		}
	}
	db.TableCreate(&recType{})
	var list []recType
	for j := int64(0); j < 5; j++ {
		list = append(list, recType{0, j, j + 1, j + 2})
	}
	db.Insert(list)
	show("All records after Insert()")
	db.Delete(&recType{}, "WHERE A == ?1 || A == ?2", int64(0), int64(4))
	show("All records after Delete()")
	db.Close()
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// All records after Insert()
	// 0 1 2
	// 1 2 3
	// 2 3 4
	// 3 4 5
	// 4 5 6
	// All records after Delete()
	// 1 2 3
	// 2 3 4
	// 3 4 5
}

// This example demonstrates table truncation.
func ExampleDbType_07() {
	type recType struct {
		ID      int64 `ql_table:"rec"`
		A, B, C int64 `ql:"*"`
	}
	db := qlm.DbCreate("data/example.ql")
	show := func(str string) {
		var rs []recType
		db.Retrieve(&rs, "ORDER BY A")
		fmt.Printf("%s\n", str)
		for _, r := range rs {
			fmt.Printf("%d %d %d\n", r.A, r.B, r.C)
		}
	}
	db.TableCreate(&recType{})
	var list []recType
	for j := int64(0); j < 5; j++ {
		list = append(list, recType{0, j, j + 1, j + 2})
	}
	db.Insert(list)
	show("All records after Insert()")
	db.Truncate(&recType{})
	show("All records after Truncate()")
	db.Close()
	if db.Err() {
		fmt.Println(db.Error())
	}
	// Output:
	// All records after Insert()
	// 0 1 2
	// 1 2 3
	// 2 3 4
	// 3 4 5
	// 4 5 6
	// All records after Truncate()
}

// This example demonstrates using ql to open and close the database. This
// could be useful if the ql database needs to be opened with special options.
func ExampleDbType_08() {
	dbFileStr := "data/example.ql"
	type recType struct {
		ID      int64 `ql_table:"sample"`
		A, B, C int64 `ql:"*"`
	}
	os.Remove(dbFileStr)
	hnd, err := ql.OpenFile(dbFileStr, &ql.Options{CanCreate: true})
	if err == nil {
		db := qlm.DbSetHandle(hnd)
		db.TableCreate(&recType{})
		var list []recType
		for j := int64(0); j < 3; j++ {
			list = append(list, recType{0, j, j + 1, j + 2})
		}
		db.Retrieve(&list, "ORDER BY A DESC")
		for _, r := range list {
			fmt.Printf("%d %d %d\n", r.A, r.B, r.C)
		}
		hnd.Close()
		err = db.Error()
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// 0 1 2
	// 1 2 3
	// 2 3 4
}

// This example is a menagerie of calls that exercise various failure code
// paths. It is a catchall of routines needed for complete test coverage using
// the go cover tool.
func ExampleDbType_09() {
	type recType struct {
		ID      int64 `ql_table:"rec"`
		A, B, C int64 `ql:"*"`
	}
	var db *qlm.DbType
	report := func() {
		if db.Err() {
			fmt.Println(db.Error())
		}
		db.ClearError()
	}
	var rl []recType
	var rec recType
	db = qlm.DbCreate("data/foo/bar/baz/example.ql")
	db.Close()
	report()
	os.RemoveAll("data/foo")
	db = qlm.DbCreate("data/example.ql")
	db.SetErrorf("application %s", "error")
	err := db.Error()
	// The following several calls exercise the quick return on existing error
	// condition
	db.Exec("foo")
	db.TableCreate(&rec)
	db.Update(&rec)
	db.Retrieve(&rl, "")
	db.Insert(rl)
	db.Delete(&rec, "")
	db.Truncate(&rec)
	report()
	db.SetError(err)
	report()
	db.Update(&rec)
	report()
	db.Insert(&rec)
	report()
	db.Retrieve(&rec, "")
	report()
	db.Retrieve(rec, "")
	report()
	db.TransactCommit()
	report()
	db.TransactRollback()
	report()
	db.Trace(true)
	db.Exec("foo")
	db.Trace(false)
	report()
	type aType struct {
		ID  bool  `ql_table:"a"`
		Val int64 `ql:"*"`
	}
	db.TableCreate(&aType{})
	report()
	type bType struct {
		ID  int64 `ql_table:"b"`
		Val int64
	}
	db.TableCreate(&bType{})
	report()
	var a int
	db.TableCreate(a)
	report()
	db.TableCreate(&a)
	report()
	type cType struct {
		ID  int64      `ql_table:"b"`
		Hnd qlm.DbType `ql:"*"`
	}
	db.TableCreate(&cType{})
	report()
	type dType struct {
		ID1 int64 `ql_table:"d1"`
		ID2 int64 `ql_table:"d2"`
		Val int64 `ql:"*"`
	}
	db.TableCreate(&dType{})
	report()
	type eType struct {
		Val int64 `ql:"*"`
	}
	db.TableCreate(&eType{})
	report()
	// Output:
	// application error
	// application error
	// at least one field name expected in function Update
	// function Insert requires slice as first argument
	// function Retrieve expecting pointer to slice, got pointer to struct
	// function Retrieve expecting pointer to slice, got struct
	// no transaction to commit
	// no transaction to rollback
	// QL [--E] foo
	// 1:1 syntax error
	// expecting int64 for id, got bool
	// no structure fields have "ql" tag
	// expecting record pointer, got int
	// specified address must be of structure with one or more fields that have a "ql" tag
	// database does not support fields of type qlm.DbType
	// multiple occurrence of ql_table tag
	// missing "ql_table" tag
}
