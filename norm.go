package norm

import (
	"context"
	"errors"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrNotAffected = errors.New("not affected by create/update")
)

type creator[Model, Args any] interface {
	Create(ctx context.Context, key Args, value Model) error
}

type reader[Model, Args any] interface {
	Read(ctx context.Context, args Args) (value Model, err error)
}

type updater[Model, Args any] interface {
	Update(ctx context.Context, args Args, value Model) error
}

type deleter[Model, Args any] interface {
	Delete(ctx context.Context, args Args) error
}

type Object[Model, Args any] struct {
	creator[Model, Args]
	reader[Model, Args]
	updater[Model, Args]
	deleter[Model, Args]
}

func NewObject[Model, Args any](
	c creator[Model, Args],
	r reader[Model, Args],
	u updater[Model, Args],
	d deleter[Model, Args],
) Object[Model, Args] {
	return Object[Model, Args]{c, r, u, d}
}

type PersistentObject[Model, Args any] struct {
	creator[Model, Args]
	reader[Model, Args]
	updater[Model, Args]
}

func NewPersistentObject[Model, Args any](
	c creator[Model, Args],
	r reader[Model, Args],
	u updater[Model, Args],
) PersistentObject[Model, Args] {
	return PersistentObject[Model, Args]{c, r, u}
}

type ImmutableObject[Model, Args any] struct {
	creator[Model, Args]
	reader[Model, Args]
}

func NewImmutableObject[Model, Args any](
	c creator[Model, Args],
	r reader[Model, Args],
) ImmutableObject[Model, Args] {
	return ImmutableObject[Model, Args]{c, r}
}

type View[Model, Args any] struct {
	reader[Model, Args]
}

func NewView[Model, Args any](r reader[Model, Args]) View[Model, Args] {
	return View[Model, Args]{r}
}
