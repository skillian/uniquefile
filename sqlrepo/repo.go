package sqlrepo

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/skillian/expr"
	"github.com/skillian/expr/errors"
	"github.com/skillian/expr/stream"
	"github.com/skillian/expr/stream/sqlstream"
	"github.com/skillian/uniquefile"
)

// Repo implements the uniquefile.Repo interface using a SQL back end.
type Repo struct {
	db *sqlstream.DB
}

var _ uniquefile.Repo = (*Repo)(nil)

func OpenRepo(ctx context.Context, driverName, dataSourceName string, options ...sqlstream.DBOption) (*Repo, error) {
	sqlDB, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, errors.Errorf0From(err, "failed to open SQL DB")
	}
	return NewRepo(ctx, sqlDB, options...)
}

func NewRepo(ctx context.Context, sqlDB *sql.DB, options ...sqlstream.DBOption) (*Repo, error) {
	db, err := sqlstream.NewDB(sqlDB, options...)
	if err != nil {
		sb := strings.Builder{}
		for i, opt := range options {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprint(opt))
		}
		return nil, errors.Errorf2From(
			err, "failed to create sqlstream DB from %v "+
				"with options: %v",
			sqlDB, sb.String(),
		)
	}
	r := &Repo{db: db}
	return r, nil
}

func (r *Repo) DB() *sqlstream.DB { return r.db }

func (r *Repo) Indications(ctx context.Context, u uniquefile.URI) (ui *uniquefile.Indication, Err error) {
	ctx, _, catcher, err := r.db.WithTx(ctx)
	if err != nil {
		return nil, errors.Errorf0From(
			err, "failed to start new transaction",
		)
	}
	defer catcher(&Err)
	uriVar := expr.NewNamedVar("resourceUri")
	var res Resource
	resQry := stream.LineOf2(r.db.Query(ctx, &res))()
	var ind Indication
	indQry := stream.LineOf2(r.db.Query(ctx, &ind))(
		func(q stream.Line) stream.Line {
			return q.Join(resQry, expr.Eq{
				expr.MemOf(q.Var(), &ind, &ind.ResourceID),
				expr.MemOf(resQry.Var(), &res, &res.ResourceID),
			}, expr.Set{q.Var(), resQry.Var()})
		},
	)
	ctx, vs := expr.ValuesFromContextOrNew(ctx)
	uriStr := u.String()
	if err := vs.Set(uriVar.Var(), uriStr); err != nil {
		return nil, err
	}
	if err := vs.Set(indQry.Var(), &ind); err != nil {
		return nil, err
	}
	lookup := uniquefile.IndicationLookup{}
	if err := stream.Each(ctx, indQry, func(c context.Context, s stream.Stream) error {
		lookup[uniquefile.Bytes(ind.Key)] = ind.Value
		return nil
	}); err != nil {
		return nil, errors.Errorf1From(
			err, "failed to retrieve indications from "+
				"resource with URI: %q",
			uriStr,
		)
	}
	ui = uniquefile.NewIndication()
	lookup.WriteToIndication(ui)
	return ui, nil
}

func (r *Repo) SetIndications(ctx context.Context, u uniquefile.URI, ui *uniquefile.Indication) (Err error) {
	lu, err := ui.Lookup()
	if err != nil {
		return err
	}
	ctx, _, catcher, err := r.db.WithTx(ctx)
	if err != nil {
		return errors.Errorf0From(
			err, "failed to begin transaction to store indications",
		)
	}
	defer catcher(&Err)
	var res Resource
	resQry := stream.LineOf2(r.db.Query(ctx, &res))(
		func(l stream.Line) stream.Line {
			return l.Filter(expr.Eq{
				expr.MemOf(l.Var(), &res, &res.Uri),
				u.String(),
			})
		},
	)
	ctx, vs := expr.ValuesFromContextOrNew(ctx)
	_ = vs.Set(resQry.Var(), &res)
	if err := stream.Single(ctx, resQry, stream.JustNext); err != nil {
		return errors.Errorf1From(
			err, "error querying for result with URI: %v",
			u,
		)
	}
	var ind Indication
	if res.ResourceID == (ResourceID{}) {
		res.Uri = u.String()
		if err := r.db.Save(ctx, &res); err != nil {
			return errors.Errorf1From(
				err, "failed to save resource: %v",
				u,
			)
		}
	} else {
		indQry := stream.LineOf2(r.db.Query(ctx, &ind))(
			func(q stream.Line) stream.Line {
				return q.Filter(expr.Eq{
					expr.MemOf(q.Var(), &ind, &ind.ResourceID),
					res.ResourceID.Value, // TODO: Make the raw structs work.
				})
			},
		)
		deletingIndication := make([]Indication, 0, 8)
		if err := stream.Each(ctx, indQry, func(c context.Context, s stream.Stream) error {
			k := uniquefile.Bytes(ind.Key)
			if v, ok := lu[k]; ok && bytes.Equal(ind.Value, v) {
				// don't re-insert the same indication:
				delete(lu, k)
				return nil
			}
			deletingIndication = append(deletingIndication, ind)
			return nil
		}); err != nil {
			return errors.Errorf2From(
				err, "failed to determine existing "+
					"indications for resource %v "+
					"(URI: %v)",
				res.ResourceID.Value, u,
			)
		}
		deleting := make([]interface{}, len(deletingIndication))
		for i := range deletingIndication {
			deleting[i] = &deletingIndication[i]
		}
		if err := r.db.Delete(ctx, deleting...); err != nil {
			return errors.Errorf0From(
				err, "failed to delete existing indications",
			)
		}
	}
	creatingIndications := make([]interface{}, 0, len(lu))
	for k, v := range lu {
		creatingIndications = append(creatingIndications, &Indication{
			ResourceID: res.ResourceID,
			Key:        string(k),
			Value:      v,
		})
	}
	if err := r.db.Save(ctx, creatingIndications...); err != nil {
		return errors.Errorf2From(
			err, "failed to save new indications for "+
				"resource %v (URI: %v)",
			res.ResourceID, u,
		)
	}
	return nil
}

func (r *Repo) URIs(ctx context.Context, query expr.Expr) (uris []uniquefile.URI, Err error) {
	var ind Indication
	indQry := stream.LineOf2(r.db.Query(ctx, &ind))()
	var res Resource
	resQry := stream.LineOf2(r.db.Query(ctx, &res))(
		func(q stream.Line) stream.Line {
			return indQry.Join(q, expr.Eq{
				expr.MemOf(indQry.Var(), &ind, &ind.ResourceID),
				expr.MemOf(q.Var(), &res, &res.ResourceID),
			}, q.Var())
		},
	)
	// elem[0] is the original query expression
	// elem[1:3] are the rewritten subexpressions (binary expressions only)
	stack := make([][3]expr.Expr, 1, 8)
	// After inspecting, stack[0][1] should hold the finished SQL
	// expression.
	_ = expr.Inspect(query, func(e expr.Expr) bool {
		if e != nil {
			stack = append(stack, [3]expr.Expr{e, nil, nil})
			return true
		}
		es := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		sec := &stack[len(stack)-1]
		top := &(*sec)[1]
		if *top != nil {
			top = &(*sec)[2]
			if *top != nil {
				Err = errors.Aggregate(Err, errors.Errorf0(
					"non-binary expressions are "+
						"not supported in this "+
						"context",
				))
				return false
			}
		}
		switch e := es[0].(type) {
		case *uniquefile.Indication:
			if err := e.Each(func(key, value []byte) error {
				indExpr := expr.And{
					expr.Eq{
						expr.MemOf(indQry.Var(), &ind, &ind.Key),
						string(key),
					},
					expr.Eq{
						expr.MemOf(indQry.Var(), &ind, &ind.Value),
						value,
					},
				}
				if *top == nil {
					*top = indExpr
					return nil
				}
				*top = expr.And{
					*top,
					indExpr,
				}
				return nil
			}); err != nil {
				Err = errors.Aggregate(Err, err)
				return false
			}
		case expr.And:
			*top = expr.And{es[1], es[2]}
		case expr.Or:
			*top = expr.Or{es[1], es[2]}
		default:
			Err = errors.Aggregate(Err, errors.Errorf1(
				"invalid expression: %[1]v "+
					"(type: %[1]T)",
				e,
			))
			return false
		}
		return true
	})
	if Err != nil {
		return
	}
	ctx, vs := expr.ValuesFromContextOrNew(ctx)
	_ = vs.Set(resQry.Var(), &res)
	if err := stream.Each(ctx, resQry, func(c context.Context, s stream.Stream) error {
		u := uniquefile.URI{}
		if err := u.FromString(res.Uri); err != nil {
			return err
		}
		uris = append(uris, u)
		return nil
	}); err != nil {
		return nil, err
	}
	return
}
