package norm

import (
	"context"
	"fmt"
)

type Merge[M1, M2 any] struct {
	L M1
	R []M2
}

type Keyer[K comparable] interface {
	Key() K
}

// Lookup performs left outer join.
func Lookup[
	M1 ~[]T1,
	M2 ~[]T2,
	T1, T2 Keyer[K],
	A1, A2 any,
	K comparable,
](
	ctx context.Context,
	lhs Reader[M1, A1], lhsArgs A1,
	rhs Reader[M2, A2], rhsArgs A2,
) (
	merge []Merge[T1, T2],
	err error,
) {
	l, err := lhs.Read(ctx, lhsArgs)
	if err != nil {
		return nil, fmt.Errorf("read lhs: %w", err)
	}

	r, err := rhs.Read(ctx, rhsArgs)
	if err != nil {
		return nil, fmt.Errorf("read rhs: %w", err)
	}

	return _lookup[M1, M2, T1, T2, K](l, r), nil
}

// Group performs values grouping.
func Group[
	M ~[]T,
	T Keyer[K],
	A any,
	K comparable,
](
	ctx context.Context,
	r Reader[M, A],
	args A,
) (
	groups map[K][]T,
	err error,
) {
	values, err := r.Read(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("read lhs: %w", err)
	}
	return _group[M, T, K](values)
}

func _lookup[
	M1 ~[]T1,
	M2 ~[]T2,
	T1 Keyer[K],
	T2 Keyer[K],
	K comparable,
](
	lhs M1,
	rhs M2,
) (
	out []Merge[T1, T2],
) {
	out = make([]Merge[T1, T2], 0, len(lhs))
	m := make(map[K][]int, len(rhs))

	for i, v := range rhs {
		k := v.Key()
		m[k] = append(m[k], i)
	}

	for _, l := range lhs {
		merge := Merge[T1, T2]{
			L: l,
			R: nil,
		}

		rii, ok := m[l.Key()]
		if ok {
			for _, i := range rii {
				merge.R = append(merge.R, rhs[i])
			}
		}

		out = append(out, merge)
	}

	return out
}

func _group[
	M ~[]T,
	T Keyer[K],
	K comparable,
](
	values M,
) (
	groups map[K][]T,
	err error,
) {
	groups = make(map[K][]T, len(values))
	for _, v := range values {
		k := v.Key()
		groups[k] = append(groups[k], v)
	}
	return
}
