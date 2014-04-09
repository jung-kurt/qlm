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

package qlm

import (
	"fmt"
	"github.com/cznic/ql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"unsafe"
)

var typeMap = map[string]bool{
	"bigint":     true,
	"bigrat":     true,
	"blob":       true,
	"bool":       true,
	"byte":       true,
	"complex128": true,
	"complex64":  true,
	"duration":   true,
	"float":      true,
	"float32":    true,
	"float64":    true,
	"int":        true,
	"int16":      true,
	"int32":      true,
	"int64":      true,
	"int8":       true,
	"rune":       true,
	"string":     true,
	"time":       true,
	"uint":       true,
	"uint16":     true,
	"uint32":     true,
	"uint64":     true,
	"uint8":      true,
}

type transactType struct {
	ctx  *ql.TCtx
	nest int
}

type qlDscType struct {
	tblStr  string
	idSf    reflect.StructField
	recTp   reflect.Type
	nameMap map[string]reflect.StructField // {"num":@ "name":@, ...}
	create  struct {
		nameTypeStr string // "num int32, name string, ..."
	}
	insert struct {
		nameStr  string   // "num, name, ..."
		nameList []string // {"num", "name", ...}
		qmStr    string   // "?1, ?2, ..."
		sfList   []reflect.StructField
	}
	sel struct {
		nameStr     string                // "id(), num, name, ..."
		sfList      []reflect.StructField // Includes ID
		typeStrList []string              // {"int64", "bigint", "string", ...}
	}
}

// DbType facilitates use of the ql database engine. Hnd is the handle to the ql instance.
type DbType struct {
	Hnd      *ql.DB
	transact transactType
	// Cache for table descriptors
	dscMap map[reflect.Type]qlDscType
	// Cache for executable commands
	listMap map[string]ql.List
	trace   bool
	err     error
}

// OK returns true if no processing errors have occurred.
func (db *DbType) OK() bool {
	return db.err == nil
}

// Err returns true if a processing error has occurred.
func (db *DbType) Err() bool {
	return db.err != nil
}

// SetError sets an error to halt database calls. This may facilitate error
// handling by application. See also OK(), Err() and Error().
func (db *DbType) SetError(err error) {
	if db.err == nil && err != nil {
		db.err = err
	}
}

// SetErrorf sets the internal Db error with formatted text to halt database
// calls. This may facilitate error handling by application.
//
// See the documentation for printing in the standard fmt package for details
// about fmtStr and args.
func (db *DbType) SetErrorf(fmtStr string, args ...interface{}) {
	if db.err == nil {
		db.err = fmt.Errorf(fmtStr, args...)
	}
}

// Error returns the internal Db error; this will be nil if no error has occurred.
func (db *DbType) Error() error {
	return db.err
}

// String satisfies the fmt.Stringer interface and returns the library name
func (db *DbType) String() string {
	return "ql/m"
}

func (db *DbType) init() {
	if db.err == nil {
		db.dscMap = make(map[reflect.Type]qlDscType)
		db.listMap = make(map[string]ql.List)
	}
}

// DbSetHandle initializes the qlm instance with a ql handle that is already
// open. This function can be used if the ql database needs to be opened with
// special options. Only one of DbSetHandle, DbOpen and DbCreate should be
// called to initialize the qlm instance. Close() may be called to close the
// specified handle after use.
func DbSetHandle(hnd *ql.DB) (db *DbType) {
	db = new(DbType)
	db.Hnd = hnd
	db.init()
	return
}

// DbOpen opens a ql database with default options. Only one of DbSetHandle,
// DbOpen and DbCreate should be called to initialize the qlm instance. After
// use, Close() should be called to free resources.
func DbOpen(dbFileStr string) (db *DbType) {
	db = new(DbType)
	db.Hnd, db.err = ql.OpenFile(dbFileStr, &ql.Options{})
	db.init()
	return
}

// DbCreate creates a new ql database with default options or overwrites an
// existing one. The directory path to the file will be created if needed. Only
// one of DbSetHandle, DbOpen and DbCreate should be called to initialize the
// qlm instance. After use, Close() should be called to free resources.
func DbCreate(dbFileStr string) (db *DbType) {
	var err error
	db = new(DbType)
	dir := filepath.Dir(dbFileStr)
	_, err = os.Stat(dir)
	if err != nil {
		db.err = os.MkdirAll(dir, 0755)
	}
	if db.err == nil {
		_, err := os.Stat(dbFileStr)
		if err == nil {
			db.err = os.Remove(dbFileStr)
		}
		if db.err == nil {
			db.Hnd, db.err = ql.OpenFile(dbFileStr, &ql.Options{CanCreate: true})
			db.init()
		}
	}
	return
}

// Close closes the qlm instance.
func (db *DbType) Close() {
	if db.Hnd != nil {
		db.Hnd.Close()
		db.Hnd = nil
	}
}

// Trace sets or unsets trace mode in which commands are printed to standard
// error. Statements that are submitted to ql for execution are printed with a
// three character flag indicating whether the command was cached (C), whether
// a transaction is pending (T), and whether an error has occurred (E).
func (db *DbType) Trace(on bool) {
	if db.err == nil {
		db.trace = on
	}
}

// TransactBegin begins a new, possibly nested, transaction. This function is
// typically not needed by applications because transactions are managed by qlm
// functions as required.
func (db *DbType) TransactBegin() {
	if db.err == nil {
		if db.transact.ctx == nil {
			db.transact.ctx = ql.NewRWCtx()
		}
		_, _ = db.Exec("BEGIN TRANSACTION;")
		if db.err == nil {
			db.transact.nest++
		}
	}
	return
}

func (db *DbType) transactEnd(ok bool) {
	var cmd, str string
	if ok {
		cmd = "COMMIT;"
		str = "commit"
	} else {
		cmd = "ROLLBACK;"
		str = "rollback"
	}
	if db.transact.nest > 0 && db.transact.ctx != nil {
		_, _ = db.Exec(cmd)
		if db.err == nil {
			db.transact.nest--
			if db.transact.nest == 0 {
				db.transact.ctx = nil
			}
		}
	} else {
		if db.err == nil {
			db.SetErrorf("no transaction to %s", str)
		}
	}
	return
}

// TransactCommit commits the pending transaction. This function is typically
// not needed by applications because transactions are managed by qlm functions
// as required.
func (db *DbType) TransactCommit() {
	if db.err == nil {
		db.transactEnd(true)
	}
	return
}

// TransactRollback rolls back the pending transaction. This function is
// typically not needed by applications because transactions are managed by qlm
// functions as required.
func (db *DbType) TransactRollback() {
	if db.err == nil {
		db.transactEnd(false)
	}
	return
}

// Exec compiles and executes a ql statement. This function is typically not
// needed by applications because various data management operations are
// handled by other qlm methods.
func (db *DbType) Exec(cmdStr string, prms ...interface{}) (rs []ql.Recordset, index int) {
	if db.err != nil {
		return
	}
	list, ok := db.listMap[cmdStr]
	if !ok {
		// Caveat: cached commands may become obsolete as different execution paths
		// result from changing database.
		list, db.err = ql.Compile(cmdStr)
		if db.err == nil {
			db.listMap[cmdStr] = list
		}
	}
	if db.err == nil {
		rs, index, db.err = db.Hnd.Execute(db.transact.ctx, list, prms...)
	}
	if db.trace {
		fmt.Fprintf(os.Stderr, "QL [%s%s%s] %s\n",
			strIf(ok, "C", "-"),
			strIf(db.transact.ctx != nil, "T", "-"),
			strIf(db.err != nil, "E", "-"),
			cmdStr)
	}
	return
}

func strIf(cond bool, aStr string, bStr string) (res string) {
	if cond {
		res = aStr
	} else {
		res = bStr
	}
	return
}

func prePad(str string) string {
	if len(str) > 0 {
		return " " + str
	}
	return str
}

func valueList(recVl reflect.Value, sfList []reflect.StructField) (list []reflect.Value) {
	addr := recVl.UnsafeAddr()
	var fldVl reflect.Value
	for _, sf := range sfList {
		// switch sf.Type {
		// case "bigrat", "bigint": // Could be a ql bug: address required
		// fldVl = reflect.NewAt(sf.Type, unsafe.Pointer(addr+sf.Offset))
		// default:
		fldVl = reflect.Indirect(reflect.NewAt(sf.Type, unsafe.Pointer(addr+sf.Offset)))
		// }
		list = append(list, fldVl)
	}
	return
}

func valList(recVl reflect.Value, sfList []reflect.StructField) (list []interface{}) {
	vlist := valueList(recVl, sfList)
	for _, v := range vlist {
		list = append(list, v.Interface())
	}
	return
}

// dscFromType collects meta information, for example field types and SQL
// names, from the passed-in record.
func (db *DbType) dscFromType(recTp reflect.Type) (dsc qlDscType) {
	if db.err != nil {
		return
	}
	if recTp.Kind() == reflect.Struct {
		var ok bool
		dsc, ok = db.dscMap[recTp]
		if !ok {
			dsc.recTp = recTp
			var sfList []reflect.StructField
			var sqlStr, tblStr, typeStr string
			var fldTp reflect.Type
			var selList, qmList, createList []string
			dsc.nameMap = make(map[string]reflect.StructField)
			for j := 0; j < recTp.NumField(); j++ {
				sfList = append(sfList, recTp.Field(j))
			}
			for j, sf := range sfList {
				if db.err == nil {
					sf = dsc.recTp.Field(j)
					fldTp = sf.Type
					sqlStr = sf.Tag.Get("ql")
					if len(sqlStr) > 0 {
						if sqlStr == "*" {
							sqlStr = sf.Name
						}
						typeStr = fmt.Sprintf("%v", fldTp)
						switch typeStr {
						case "time.Time":
							typeStr = "time"
						case "time.Duration":
							typeStr = "duration"
						case "big.Rat":
							typeStr = "bigrat"
						case "big.Int":
							typeStr = "bigint"
						case "[]uint8":
							typeStr = "blob"
						}
						dsc.nameMap[sqlStr] = sf
						strListAppend(&createList, "%s %s", sqlStr, typeStr)
						dsc.insert.sfList = append(dsc.insert.sfList, sf)
						strListAppend(&dsc.insert.nameList, "%s", sqlStr)
						strListAppend(&qmList, "?%d", len(dsc.insert.sfList))
						strListAppend(&dsc.sel.typeStrList, "%s", typeStr)
						strListAppend(&selList, "%s", sqlStr)
						dsc.sel.sfList = append(dsc.sel.sfList, sf)
						if !typeMap[typeStr] {
							db.SetErrorf("database does not support fields of type %s", typeStr)
						}
					} else {
						tblStr = sf.Tag.Get("ql_table")
						if len(tblStr) > 0 {
							if len(dsc.tblStr) == 0 {
								if fldTp.Kind() == reflect.Int64 {
									strListAppend(&selList, "id()")
									dsc.sel.sfList = append(dsc.sel.sfList, sf)
									strListAppend(&dsc.sel.typeStrList, "%v", sf.Type.Kind())
									dsc.tblStr = tblStr
									dsc.idSf = sf
								} else {
									db.SetErrorf("expecting int64 for id, got %v", fldTp.Kind())
								}
							} else {
								db.SetErrorf("duplicate occurrence of ql_table tag")
							}
						}
					}
				}
			}
			if db.err == nil {
				if len(dsc.insert.sfList) == 0 {
					db.SetErrorf(`no structure fields have "ql" tag`)
				} else if len(dsc.tblStr) == 0 {
					db.SetErrorf(`missing "ql_table" tag`)
				} else {
					dsc.insert.qmStr = strings.Join(qmList, ", ")
					dsc.insert.nameStr = strings.Join(dsc.insert.nameList, ", ")
					dsc.create.nameTypeStr = strings.Join(createList, ", ")
					dsc.sel.nameStr = strings.Join(selList, ", ")
					db.dscMap[recTp] = dsc // cache
					// dump(dsc)
				}
			}
		}
	} else {
		db.SetErrorf(`specified address must be of structure with ` +
			`one or more fields that have a "ql" tag`)
	}
	return
}

// Function dsc collects meta information, for example field types and SQL
// names, from the passed-in record.
func (db *DbType) dscFromPtr(recPtr interface{}) (dsc qlDscType) {
	ptrVl := reflect.ValueOf(recPtr)
	kd := ptrVl.Kind()
	if kd == reflect.Ptr {
		return db.dscFromType(ptrVl.Elem().Type())
	}
	db.SetErrorf("expecting record pointer, got %v", kd)
	return
}

func strListAppend(listPtr *[]string, fmtStr string, args ...interface{}) {
	*listPtr = append(*listPtr, fmt.Sprintf(fmtStr, args...))
}

// TableCreate creates a table based strictly on the "ql" and "ql_table" tags
// in the type definition of the specified record. The table is overwritten if
// it already exists.
func (db *DbType) TableCreate(recPtr interface{}) {
	if db.err != nil {
		return
	}
	// DROP TABLE IF EXISTS foo
	// CREATE TABLE foo (num int32, name string)
	var dsc qlDscType
	dsc = db.dscFromPtr(recPtr)
	if db.err == nil {
		// Consider supporting flag that controls how existing table is handled
		// (function fail or table overwritten)
		db.TransactBegin()
		if db.err == nil {
			cmd := fmt.Sprintf("DROP TABLE IF EXISTS %s;", dsc.tblStr)
			_, _ = db.Exec(cmd)
			if db.err == nil {
				cmd = fmt.Sprintf("CREATE TABLE %s (%s);", dsc.tblStr, dsc.create.nameTypeStr)
				// fmt.Printf("QL [%s]\n", cmd)
				_, _ = db.Exec(cmd)
			}
		}
		db.transactEnd(db.err == nil)
	}
	return
}

// Update updates the specified record in the database. The ID field (tagged
// with "ql_table" in the structure definition) is used to identify the record
// in the table. It must have the same value as it had when the record was
// retrieved from the database using Retrieve. fldNames specify the fields that
// will be updated. The field names are the ones used in the database, that is,
// the names identified with the "ql" tag in the structure definition. If the
// first string is "*", all fields are updated. Unmatched field names result in
// an error.
func (db *DbType) Update(recPtr interface{}, fldNames ...string) {
	if db.err != nil {
		return
	}
	// UPDATE foo name = ?1, num = ?2 WHERE id() == ?3;
	if len(fldNames) > 0 {
		var dsc qlDscType
		dsc = db.dscFromPtr(recPtr)
		if db.err == nil {
			recVl := reflect.ValueOf(recPtr).Elem()
			addr := recVl.UnsafeAddr()
			var args []interface{}
			var eqList []string
			var sf reflect.StructField
			if fldNames[0] == "*" {
				fldNames = dsc.insert.nameList
			}
			pos := 0
			for _, nm := range fldNames {
				// fmt.Printf("sf.Name [%s], %v\n", sf.Name, fldMap[sf.Name])
				pos++
				sf = dsc.nameMap[nm]
				strListAppend(&eqList, "%s = ?%d", nm, pos)
				args = append(args, reflect.Indirect(
					reflect.NewAt(sf.Type, unsafe.Pointer(addr+sf.Offset))).Interface())
			}
			args = append(args, reflect.Indirect(
				reflect.NewAt(dsc.idSf.Type, unsafe.Pointer(addr+dsc.idSf.Offset))).Interface())
			db.TransactBegin()
			if db.err == nil {
				cmd := fmt.Sprintf("UPDATE %s %s WHERE id() == ?%d;", dsc.tblStr,
					strings.Join(eqList, ", "), pos+1)
				_, _ = db.Exec(cmd, args...)
			}
			db.transactEnd(db.err == nil)
		}
	} else {
		db.SetErrorf("at least one field name expected")
	}
	return
}

// Delete removes all records from the database that satisfy the specified tail
// clause and its arguments. For example, if tailStr is empty, all records from
// the table will be deleted.
func (db *DbType) Delete(recPtr interface{}, tailStr string, prms ...interface{}) {
	if db.err != nil {
		return
	}
	// DELETE FROM foo WHERE a > ?1 AND b < ?2
	var dsc qlDscType
	dsc = db.dscFromPtr(recPtr)
	if db.err == nil {
		db.TransactBegin()
		if db.err == nil {
			cmd := fmt.Sprintf("DELETE FROM %s%s;", dsc.tblStr, prePad(tailStr))
			_, _ = db.Exec(cmd, prms...)
		}
		db.transactEnd(db.err == nil)
	}
}

// Insert stores in the database the records included in the specified slice.
// The value of the ID field that is tagged with "ql_table" is ignored.
func (db *DbType) Insert(slice interface{}) {
	if db.err != nil {
		return
	}
	var dsc qlDscType
	var vList []interface{}
	sliceVl := reflect.ValueOf(slice)
	sliceTp := sliceVl.Type()
	if sliceTp.Kind() == reflect.Slice {
		count := sliceVl.Len()
		recTp := sliceTp.Elem()
		dsc = db.dscFromType(recTp)
		if db.err == nil {
			cmdStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
				dsc.tblStr, dsc.insert.nameStr, dsc.insert.qmStr)
			// fmt.Printf("QL [%s]\n", cmdStr)
			var recVl reflect.Value
			db.TransactBegin()
			for recJ := 0; recJ < count && db.err == nil; recJ++ { // Record loop
				recVl = sliceVl.Index(recJ)
				vList = valList(recVl, dsc.insert.sfList)
				_, _ = db.Exec(cmdStr, vList...)
				// dump(valList)
			}
			db.transactEnd(db.err == nil)
		}
	} else {
		db.SetErrorf("function Insert requires slice as first argument")
	}
}

// Retrieve selects zero or more records of the type pointed to by slicePtr
// from the database. The retrieved records are appended to the slice. If the
// retrieved records are to repopulate the slice instead, assign nil to the
// slice prior to calling this function. tailStr is intended to include a WHERE
// clause. For every parameter token ("?1", "?2", etc) in the string, a
// suitable expression list (one-based) after the tail string should be passed.
func (db *DbType) Retrieve(slicePtr interface{}, tailStr string, prms ...interface{}) {
	if db.err != nil {
		return
	}
	var dsc qlDscType
	slicePtrVl := reflect.ValueOf(slicePtr)
	kd := slicePtrVl.Kind()
	if kd == reflect.Ptr {
		sliceVl := reflect.Indirect(slicePtrVl)
		kd = sliceVl.Kind()
		if kd == reflect.Slice {
			sliceTp := sliceVl.Type()
			recTp := sliceTp.Elem()
			dsc = db.dscFromType(recTp)
			if db.err == nil {
				cmdStr := fmt.Sprintf("SELECT %s FROM %s%s;",
					dsc.sel.nameStr, dsc.tblStr, prePad(tailStr))
				// fmt.Printf("QL [%s]\n", cmdStr)
				var rs []ql.Recordset
				rs, _ = db.Exec(cmdStr, prms...)
				if db.err == nil {
					recVl := reflect.Indirect(reflect.New(recTp)) // Buffer
					vList := valueList(recVl, dsc.sel.sfList)
					var v reflect.Value
					load := func(data []interface{}) (more bool, err error) {
						for j, f := range data {
							switch dsc.sel.typeStrList[j] {
							case "bigrat", "bigint":
								v = reflect.Indirect(reflect.ValueOf(f))
							default:
								v = reflect.ValueOf(f)
							}
							// fmt.Printf("%2d: %s [%v] %v\n", j, dsc.fld.nameList[j], vList[j], f)
							vList[j].Set(v)
						}
						// dump("result", data)
						sliceVl = reflect.Append(sliceVl, recVl)
						more = true
						return
					}
					for _, res := range rs {
						if db.err == nil {
							db.err = res.Do(false, load)
						}
					}
					if db.err == nil {
						// Assign sliceVl back to *slicePtr
						reflect.Indirect(slicePtrVl).Set(sliceVl)
					}
				}
			}
		} else {
			db.SetErrorf("expecting pointer to slice, got pointer to %v", kd)

		}
	} else {
		db.SetErrorf("expecting pointer to slice, got %v", kd)
	}
	return
}
