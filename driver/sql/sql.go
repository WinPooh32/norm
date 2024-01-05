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

type Object[M, A any] struct {
	creator[M, A]
	reader[M, A]
	updater[M, A]
	deleter[M, A]
}

type View[M, A any] struct {
	reader[M, A]
}

func NewObject[M, A any](db *sql.DB, c, r, u, d string) Object[M, A] {
	var m M

	return Object[M, A]{
		creator: creator[M, A]{writer[M, A]{db, c}},
		reader:  reader[M, A]{db, r, isSlice(m)},
		updater: updater[M, A]{writer[M, A]{db, u}},
		deleter: deleter[M, A]{writer[M, A]{db, d}},
	}
}

func NewView[M, A any](db *sql.DB, r string) View[M, A] {
	var m M
	return View[M, A]{
		reader: reader[M, A]{
			db:        db,
			tpl:       r,
			scanSlice: isSlice(m),
		},
	}
}

type preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

type a[A any] struct {
	A A
}

type ma[M, A any] struct {
	M M
	A A
}

type creator[M, A any] struct {
	writer[M, A]
}

func (c creator[M, A]) Create(ctx context.Context, args A, value M) error {
	return c.affect(ctx, args, value)
}

type updater[M, A any] struct {
	writer[M, A]
}

func (u updater[M, A]) Update(ctx context.Context, args A, value M) error {
	return u.affect(ctx, args, value)
}

type deleter[M, A any] struct {
	writer[M, A]
}

func (d deleter[M, A]) Delete(ctx context.Context, args A) error {
	var nop M
	return d.affect(ctx, args, nop)
}

type writer[M, A any] struct {
	db  *sql.DB
	tpl string
}

func (w writer[M, A]) affect(ctx context.Context, args A, value M) error {
	pr := newPreparer(txValue(ctx), w.db)
	if err := w.exec(ctx, pr, args, value); err != nil {
		return err
	}

	return nil
}

func (w writer[M, A]) exec(ctx context.Context, pr preparer, args A, value M) error {
	stmtRaw, stmtA, err := tq.Compile(w.tpl, ma[M, A]{M: value, A: args})
	if err != nil {
		return fmt.Errorf("compile query template: %w", err)
	}

	stmt, err := pr.PrepareContext(ctx, stmtRaw)
	if err != nil {
		return fmt.Errorf("prepare query: %w", err)
	}

	res, err := stmt.ExecContext(ctx, stmtA...)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err == nil && n <= 0 {
		return norm.ErrNotAffected
	}

	return nil
}

type reader[M, A any] struct {
	db        *sql.DB
	tpl       string
	scanSlice bool
}

func (r reader[M, A]) Read(ctx context.Context, args A) (value M, err error) {
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

func (r reader[M, A]) query(ctx context.Context, pr preparer, args A) (rows *sql.Rows, err error) {
	stmtRaw, stmtA, err := tq.Compile(r.tpl, a[A]{A: args})
	if err != nil {
		return nil, fmt.Errorf("compile query template: %w", err)
	}

	stmt, err := pr.PrepareContext(ctx, stmtRaw)
	if err != nil {
		return nil, fmt.Errorf("prepare query: %w", err)
	}

	rows, err = stmt.QueryContext(ctx, stmtA...)
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
