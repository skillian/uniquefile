package uniquefile

import (
	"context"

	"github.com/skillian/expr"
)

type Repo interface {
	// Indications retrieves the indication(s) associated with the
	// given URI (if any).
	Indications(ctx context.Context, u URI) (*Indication, error)

	// SetIndications adds (or replaces) the URI's indications with
	// those provided.
	SetIndications(ctx context.Context, u URI, ind *Indication) error

	// URIs returns zero or more URIs that match the queried
	// Indications.  query can be a single indication or any
	// hierarchy of expr.And or expr.Or expressions whose leaves
	// are Indications.
	URIs(ctx context.Context, query expr.Expr) ([]URI, error)
}
