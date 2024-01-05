package norm

import (
	"context"
	"errors"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrNotAffected = errors.New("not affected by create/update")
)

type Creator[M, A any] interface {
	Create(ctx context.Context, key A, value M) error
}

type Reader[M, A any] interface {
	Read(ctx context.Context, args A) (value M, err error)
}

type Updater[M, A any] interface {
	Update(ctx context.Context, args A, value M) error
}

type Deleter[M, A any] interface {
	Delete(ctx context.Context, args A) error
}

type Object[M, A any] interface {
	Creator[M, A]
	Reader[M, A]
	Updater[M, A]
	Deleter[M, A]
}

type PersistentObject[M, A any] interface {
	Creator[M, A]
	Reader[M, A]
	Updater[M, A]
}

type ImmutableObject[M, A any] interface {
	Creator[M, A]
	Reader[M, A]
}

type View[M, A any] interface {
	Reader[M, A]
}
