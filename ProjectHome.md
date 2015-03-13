## Introduction ##

Package qlm implements a simple, high-level wrapper for ql, a native, embedded
database engine written in Go. Each table in the database is associated with an
application-defined structure in Go. These structures contain special tags that
allow qlm to automatically manage all database operations.

The ql website is https://github.com/cznic/ql

## License ##

qlm is copyrighted by Kurt Jung and is released under the MIT License.

## Installation ##

To install the package on your system, run
```
go get code.google.com/p/qlm
```
Later, to receive updates, run
```
go get -u code.google.com/p/qlm
```
## Quick Start ##

The following Go code demonstrates the creation of a database, the creation of
a table within that database, and subsequent operations.
```
type recType struct {
	ID   int64  `ql_table:"rec"`
	Name string `ql:"*"`
}
db := DbCreate("data/simple.ql")
db.TableCreate(&recType{})
db.Insert([]recType{{0, "Athos"}, {0, "Porthos"}, {0, "Aramis"}})
var list []recType
db.Retrieve(&list, "WHERE Name[0:1] == ?1", "A")
for _, r := range list {
	fmt.Println(r.Name)
}
db.Close()
```
## Documentation ##

[![](https://godoc.org/code.google.com/p/qlm?status.png)](https://godoc.org/code.google.com/p/qlm)