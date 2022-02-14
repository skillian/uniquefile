module github.com/skillian/uniquefile

go 1.16

replace github.com/skillian/argparse => ../argparse

replace github.com/skillian/logging => ../logging

replace github.com/skillian/expr => ../expr

require (
	github.com/alexbrainman/odbc v0.0.0-20211220213544-9c9a2e61c5e2 // indirect
	github.com/denisenkom/go-mssqldb v0.12.0 // indirect
	github.com/mattn/go-sqlite3 v1.14.6
	github.com/skillian/argparse v0.0.0-20220107121831-4b430f804d62
	github.com/skillian/errors v0.0.0-20190910214200-f19f31b303bd
	github.com/skillian/expr v0.0.0-20211220223747-6b7ac90b91f2
	github.com/skillian/logging v0.0.0-20210406222847-057884e2cfcc
)
