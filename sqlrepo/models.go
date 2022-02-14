package sqlrepo

import (
	"github.com/skillian/expr/stream/sqlstream"
	"github.com/skillian/expr/stream/sqlstream/sqltypes"
)

type ResourceID struct {
	Value int64
}

func (id *ResourceID) AppendFields(fs []interface{}) []interface{} {
	return append(fs, &id.Value)
}

func (id ResourceID) AppendValues(vs []interface{}) []interface{} {
	return append(vs, id.Value)
}

func (id ResourceID) AppendSQLTypes(ts []sqltypes.Type) []sqltypes.Type {
	return append(ts, sqltypes.IntType{Bits: 64})
}

type Resource struct {
	ResourceID ResourceID
	Uri string
}

func (m *Resource) ID() sqlstream.Model {
	return sqlstream.ModelWithNames(&m.ResourceID, "ResourceID")
}

func (m *Resource) AppendFields(fs []interface{}) []interface{} {
	fs = m.ResourceID.AppendFields(fs)
	fs = append(fs, &m.Uri)
	return fs
}

var namesOfResourceFields = []string{
	"ResourceID",
	"Uri",
}

func (m Resource) AppendNames(ns []string) []string {
	return append(ns, namesOfResourceFields...)
}

func (m Resource) AppendValues(vs []interface{}) []interface{} {
	vs = m.ResourceID.AppendValues(vs)
	vs = append(vs, m.Uri)
	return vs
}

var sqlNamesOfResourceFields = []string{
	"ResourceID",
	"Uri",
}

func (m Resource) AppendSQLNames(ns []string) []string {
	return append(ns, sqlNamesOfResourceFields...)
}

var typesOfResourceFields = []sqltypes.Type{
	sqltypes.IntType{Bits: 64},
	sqltypes.StringType{Var: true, Length: 0},
}

func (m Resource) AppendSQLTypes(ts []sqltypes.Type) []sqltypes.Type {
	return append(ts, typesOfResourceFields...)
}

func (m Resource) SQLTableName() string { return "Resource" }

type IndicationID struct {
	Value int64
}

func (id *IndicationID) AppendFields(fs []interface{}) []interface{} {
	return append(fs, &id.Value)
}

func (id IndicationID) AppendValues(vs []interface{}) []interface{} {
	return append(vs, id.Value)
}

func (id IndicationID) AppendSQLTypes(ts []sqltypes.Type) []sqltypes.Type {
	return append(ts, sqltypes.IntType{Bits: 64})
}

type Indication struct {
	IndicationID IndicationID
	ResourceID ResourceID
	Key string
	Value []byte
}

func (m *Indication) ID() sqlstream.Model {
	return sqlstream.ModelWithNames(&m.IndicationID, "IndicationID")
}

func (m *Indication) AppendFields(fs []interface{}) []interface{} {
	fs = m.IndicationID.AppendFields(fs)
	fs = m.ResourceID.AppendFields(fs)
	fs = append(fs, &m.Key)
	fs = append(fs, &m.Value)
	return fs
}

var namesOfIndicationFields = []string{
	"IndicationID",
	"ResourceID",
	"Key",
	"Value",
}

func (m Indication) AppendNames(ns []string) []string {
	return append(ns, namesOfIndicationFields...)
}

func (m Indication) AppendValues(vs []interface{}) []interface{} {
	vs = m.IndicationID.AppendValues(vs)
	vs = m.ResourceID.AppendValues(vs)
	vs = append(vs, m.Key)
	vs = append(vs, m.Value)
	return vs
}

var sqlNamesOfIndicationFields = []string{
	"IndicationID",
	"ResourceID",
	"Key",
	"Value",
}

func (m Indication) AppendSQLNames(ns []string) []string {
	return append(ns, sqlNamesOfIndicationFields...)
}

var typesOfIndicationFields = []sqltypes.Type{
	sqltypes.IntType{Bits: 64},
	sqltypes.IntType{Bits: 64},
	sqltypes.StringType{Var: false, Length: 16},
	sqltypes.BytesType{Var: true, Length: 0},
}

func (m Indication) AppendSQLTypes(ts []sqltypes.Type) []sqltypes.Type {
	return append(ts, typesOfIndicationFields...)
}

func (m Indication) SQLTableName() string { return "Indication" }



