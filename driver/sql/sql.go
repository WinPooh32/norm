package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/VauntDev/tqla"
	"github.com/blockloop/scan/v2"

	"github.com/WinPooh32/norm"
)

var tq, _ = tqla.New(tqla.WithPlaceHolder(tqla.Dollar))

func SetPlaceHolder(p tqla.Placeholder) {
	tq, _ = tqla.New(tqla.WithPlaceHolder(p))
}

type txKey struct{}

func WithTransaction(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func txValue(ctx context.Context) *sql.Tx {
	tx := ctx.Value(txKey{})
	if tx == nil {
		return nil
	}
	return tx.(*sql.Tx)
}

func NewObject[Model, Args any](db *sql.DB, c, r, u, d string) norm.Object[Model, Args] {
	var m Model

	return norm.NewObject[Model, Args](
		&creator[Model, Args]{
			writer[Model, Args]{db, c},
		},
		&reader[Model, Args]{
			db, r, isSlice(m),
		},
		&updater[Model, Args]{
			writer[Model, Args]{db, u},
		},
		&deleter[Model, Args]{
			writer[Model, Args]{db, d},
		},
	)
}

func NewView[Model, Args any](db *sql.DB, r string) norm.View[Model, Args] {
	var m Model

	reader := &reader[Model, Args]{
		db:        db,
		tpl:       r,
		scanSlice: isSlice(m),
	}

	return norm.NewView[Model, Args](reader)
}

type preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

type a[Args any] struct {
	A Args
}

type ma[Model, Args any] struct {
	M Model
	A Args
}

type creator[Model, Args any] struct {
	writer[Model, Args]
}

func (c *creator[Model, Args]) Create(ctx context.Context, args Args, value Model) error {
	return c.affect(ctx, args, value)
}

type updater[Model, Args any] struct {
	writer[Model, Args]
}

func (u *updater[Model, Args]) Update(ctx context.Context, args Args, value Model) error {
	return u.affect(ctx, args, value)
}

type deleter[Model, Args any] struct {
	writer[Model, Args]
}

func (d *deleter[Model, Args]) Delete(ctx context.Context, args Args) error {
	var nop Model
	return d.affect(ctx, args, nop)
}

type writer[Model, Args any] struct {
	db  *sql.DB
	tpl string
}

func (w *writer[Model, Args]) affect(ctx context.Context, args Args, value Model) error {
	pr := newPreparer(txValue(ctx), w.db)
	if err := w.exec(ctx, pr, args, value); err != nil {
		return err
	}

	return nil
}

func (w *writer[Model, Args]) exec(ctx context.Context, pr preparer, args Args, value Model) error {
	stmtRaw, stmtArgs, err := tq.Compile(w.tpl, ma[Model, Args]{M: value, A: args})
	if err != nil {
		return fmt.Errorf("compile query template: %w", err)
	}

	stmt, err := pr.PrepareContext(ctx, stmtRaw)
	if err != nil {
		return fmt.Errorf("prepare query: %w", err)
	}

	res, err := stmt.ExecContext(ctx, stmtArgs...)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err == nil && n <= 0 {
		return norm.ErrNotAffected
	}

	return nil
}

type reader[Model, Args any] struct {
	db        *sql.DB
	tpl       string
	scanSlice bool
}

func (r *reader[Model, Args]) Read(ctx context.Context, args Args) (value Model, err error) {
	pr := newPreparer(txValue(ctx), r.db)

	rows, err := r.query(ctx, pr, args)
	if err != nil {
		return value, err
	}

	if r.scanSlice {
		err = scan.RowsStrict(&value, rows)
		if err != nil {
			return value, fmt.Errorf("scan rows: %w", err)
		}
	} else {
		err = scan.RowStrict(&value, rows)
		if errors.Is(err, sql.ErrNoRows) {
			return value, norm.ErrNotFound
		}
		if err != nil {
			return value, fmt.Errorf("scan one row: %w", err)
		}
	}

	return value, nil
}

func (r *reader[Model, Args]) query(ctx context.Context, pr preparer, args Args) (rows *sql.Rows, err error) {
	stmtRaw, stmtArgs, err := tq.Compile(r.tpl, a[Args]{A: args})
	if err != nil {
		return nil, fmt.Errorf("compile query template: %w", err)
	}

	stmt, err := pr.PrepareContext(ctx, stmtRaw)
	if err != nil {
		return nil, fmt.Errorf("prepare query: %w", err)
	}

	rows, err = stmt.QueryContext(ctx, stmtArgs...)
	if err != nil {
		return nil, fmt.Errorf("run query: %w", err)
	}

	return rows, nil
}

func isSlice(v any) bool {
	return reflect.TypeOf(v).Kind() == reflect.Slice
}

func newPreparer(tx *sql.Tx, db *sql.DB) (pr preparer) {
	if tx != nil {
		pr = tx
	} else {
		pr = db
	}
	return pr
}
