package sql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/WinPooh32/norm"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"

	"github.com/lib/pq"
)

var db *sql.DB

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "10",
		Env: []string{
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_USER=user_name",
			"POSTGRES_DB=dbname",
			"listen_addresses = '*'",
		},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	hostAndPort := resource.GetHostPort("5432/tcp")
	databaseUrl := fmt.Sprintf("postgres://user_name:secret@%s/dbname?sslmode=disable", hostAndPort)

	log.Println("Connecting to database on url: ", databaseUrl)

	resource.Expire(120) // Tell docker to hard kill the container in 120 seconds

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 120 * time.Second
	if err = pool.Retry(func() error {
		db, err = sql.Open("postgres", databaseUrl)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	//Run tests
	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func resetDB(t testing.TB, db *sql.DB) error {
	t.Helper()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	qq := []string{
		`DROP TABLE IF EXISTS "tests";`,
		`CREATE TABLE "tests" (
		"id" text PRIMARY KEY,
		"field_a" text NOT NULL,
		"field_b" text NOT NULL,
		"field_c" int NOT NULL,
		"created_at" TIMESTAMP WITH TIME ZONE NOT NULL,
		"updated_at" TIMESTAMP WITH TIME ZONE NOT NULL
		);`,
		`INSERT INTO "tests" VALUES(
			'id01', 
			'a', 
			'b', 
			1234, 
			timestamp '2001-09-28 23:00', 
			timestamp '2001-09-28 23:00'
		);`,
		`INSERT INTO "tests" VALUES(
			'id02', 
			'aaaa', 
			'bbbb', 
			4321, 
			timestamp '2002-09-28 23:00', 
			timestamp '2002-09-28 23:00'
		);`,
		`DROP TABLE IF EXISTS "tests_2";`,
		`CREATE TABLE "tests_2" (
			"id" text PRIMARY KEY,
			"field_a" text NOT NULL,
			"field_b" text NOT NULL,
			"field_c" int NOT NULL,
			"created_at" TIMESTAMP WITH TIME ZONE NOT NULL,
			"updated_at" TIMESTAMP WITH TIME ZONE NOT NULL
			);`,
		`INSERT INTO "tests_2" VALUES(
			'id03', 
			'aa00', 
			'bb00', 
			1000, 
			timestamp '2003-09-28 23:00', 
			timestamp '2003-09-28 23:00'
		);`,
	}

	for _, q := range qq {
		_, err := tx.Exec(q)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

type Model struct {
	ID        string    `db:"id"`
	FieldA    string    `db:"field_a"`
	FieldB    string    `db:"field_b"`
	FieldC    int       `db:"field_c"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type ModelShort struct {
	FieldA string `db:"field_a"`
	FieldB string `db:"field_b"`
	FieldC int    `db:"field_c"`
}

type Args struct {
	ID        string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type FilterID struct {
	ID string
}

func setupQueries() (_ *sql.DB, c string, r string, u string, d string) {
	c = `
INSERT INTO "tests" (
	"id", 
	"field_a",
	"field_b",
	"field_c",
	"created_at",
	"updated_at"
) VALUES (
	{{.A.ID}},
	{{.M.FieldA}},
	{{.M.FieldB}},
	{{.M.FieldC}},
	{{.A.CreatedAt}},
	{{.A.UpdatedAt}}
);
`

	r = `
SELECT 
	"id", 
	"field_a",
	"field_b",
	"field_c",
	"created_at",
	"updated_at"
FROM 
	"tests"
WHERE 
	"id" = {{.A.ID}}
;`

	u = `
UPDATE 
	"tests" 
SET
	"field_a" = {{.M.FieldA}},
	"field_b" = {{.M.FieldB}},
	"field_c" = {{.M.FieldC}},
	"updated_at" = {{.A.UpdatedAt}}
WHERE
	"id" = {{.A.ID}}
;
`

	d = `
DELETE 
FROM 
	"tests" 
WHERE 
	"id" = {{.A.ID}}
`

	return db, c, r, u, d
}

func setupViewQueries() (_ *sql.DB, r string) {
	return db, `
	SELECT 
		"id", 
		"field_a",
		"field_b",
		"field_c",
		"created_at",
		"updated_at"
	FROM 
		"tests" 
	WHERE 
		"id" = {{ .A.ID }}
	;`
}

type ModelEmpty struct{}

func TestObject_Create(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	modelObject := NewObject[ModelShort, Args](setupQueries())

	err := modelObject.Create(context.Background(),
		Args{
			ID:        "qwerty",
			CreatedAt: time.Date(2001, 9, 28, 23, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2001, 9, 28, 23, 0, 0, 0, time.UTC),
		},
		ModelShort{
			FieldA: "a",
			FieldB: "b",
			FieldC: 1,
		},
	)

	assert.NoError(t, err)
}

func TestObject_Create_Error_NotAffected(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	modelObject := NewObject[ModelEmpty, FilterID](
		db,
		`INSERT INTO "tests_2" (
			"id", 
			"field_a",
			"field_b",
			"field_c",
			"created_at",
			"updated_at"
		) 
		(
			SELECT
				"id", 
				"field_a",
				"field_b",
				"field_c",
				"created_at",
				"updated_at"
			FROM 
				"tests_2"
			WHERE 
				"id" = {{.A.ID}}
		)`,
		``, ``, ``,
	)

	err := modelObject.Create(context.Background(),
		FilterID{
			ID: "not found",
		},
		ModelEmpty{},
	)

	if assert.Error(t, err) {
		assert.ErrorIs(t, err, norm.ErrNotAffected)
	}
}

func TestObject_Read(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	want := ModelShort{
		FieldA: "a",
		FieldB: "b",
		FieldC: 1,
	}

	modelObject := NewObject[ModelShort, Args](setupQueries())

	err := modelObject.Create(context.Background(),
		Args{
			ID:        "qwerty",
			CreatedAt: time.Date(2001, 9, 28, 23, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2001, 9, 28, 23, 0, 0, 0, time.UTC),
		},
		ModelShort{
			FieldA: "a",
			FieldB: "b",
			FieldC: 1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	modelObject = NewObject[ModelShort, Args](setupQueries())

	got, err := modelObject.Read(context.Background(), Args{ID: "qwerty"})

	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestObject_Read_Error_NotFound(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	modelObject := NewObject[ModelShort, Args](setupQueries())

	err := modelObject.Create(context.Background(),
		Args{
			ID:        "qwerty",
			CreatedAt: time.Date(2001, 9, 28, 23, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2001, 9, 28, 23, 0, 0, 0, time.UTC),
		},
		ModelShort{
			FieldA: "a",
			FieldB: "b",
			FieldC: 1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	modelObject = NewObject[ModelShort, Args](setupQueries())

	_, err = modelObject.Read(context.Background(), Args{ID: "qwerty123"})

	if assert.Error(t, err) {
		assert.ErrorIs(t, err, norm.ErrNotFound)
	}
}

func TestObject_Update(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	want := ModelShort{
		FieldA: "updated_a",
		FieldB: "updated_b",
		FieldC: 666,
	}

	updateID := "id01"

	update := ModelShort{
		FieldA: "updated_a",
		FieldB: "updated_b",
		FieldC: 666,
	}

	modelObject := NewObject[ModelShort, Args](setupQueries())

	err := modelObject.Update(context.Background(), Args{ID: updateID}, update)
	if err != nil {
		t.Fatal(err)
	}

	modelObject = NewObject[ModelShort, Args](setupQueries())

	got, err := modelObject.Read(context.Background(), Args{ID: updateID})
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, want, got)
}

func TestObject_Update_Error_NotAffected(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	updateID := "-1"

	modelObject := NewObject[ModelShort, Args](setupQueries())

	err := modelObject.Update(context.Background(), Args{ID: updateID}, ModelShort{})

	if assert.Error(t, err) {
		assert.ErrorIs(t, err, norm.ErrNotAffected)
	}
}

func TestObject_Delete(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	deleteID := "id01"

	modelObject := NewObject[ModelShort, Args](setupQueries())

	err := modelObject.Delete(context.Background(), Args{ID: deleteID})
	if err != nil {
		t.Fatal(err)
	}

	modelObject = NewObject[ModelShort, Args](setupQueries())

	_, err = modelObject.Read(context.Background(), Args{ID: deleteID})

	if assert.Error(t, err) {
		assert.ErrorIs(t, err, norm.ErrNotFound)
	}
}

func TestObject_Delete_Error_NotAffected(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	deleteID := "-1"

	modelObject := NewObject[ModelShort, Args](setupQueries())

	err := modelObject.Delete(context.Background(), Args{ID: deleteID})

	if assert.Error(t, err) {
		assert.ErrorIs(t, err, norm.ErrNotAffected)
	}
}

func TestObject_WithTransaction(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	tx1, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback()

	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	ctxTx1 := WithTransaction(context.Background(), tx1)
	ctxTx2 := WithTransaction(context.Background(), tx2)

	id := "id01"

	want := ModelShort{
		FieldA: "a",
		FieldB: "b",
		FieldC: 1234,
	}

	modelObject := NewObject[ModelShort, Args](setupQueries())

	err = modelObject.Delete(ctxTx1, Args{ID: id})
	if err != nil {
		t.Fatal(err)
	}

	_, errReadTx1 := modelObject.Read(ctxTx1, Args{ID: id})

	got, err := modelObject.Read(ctxTx2, Args{ID: id})
	if err != nil {
		t.Fatal(err)
	}

	err = tx1.Commit()
	if err != nil {
		t.Fatal(err)
	}

	err = tx2.Commit()
	if err != nil {
		t.Fatal(err)
	}

	if assert.Error(t, errReadTx1) {
		assert.ErrorIs(t, errReadTx1, norm.ErrNotFound)
	}

	assert.Equal(t, want, got)
}

func TestView_Read(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	want := Model{
		ID:        "id01",
		FieldA:    "a",
		FieldB:    "b",
		FieldC:    1234,
		CreatedAt: time.Date(2001, 9, 28, 23, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2001, 9, 28, 23, 0, 0, 0, time.UTC),
	}

	modelView := NewView[Model, FilterID](setupViewQueries())

	got, err := modelView.Read(context.Background(), FilterID{
		ID: "id01",
	})
	if err != nil {
		t.Fatal(err)
	}

	got.CreatedAt = got.CreatedAt.UTC()
	got.UpdatedAt = got.UpdatedAt.UTC()

	assert.Equal(t, want, got)
}

func TestView_Read_Error_NotFound(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	modelView := NewView[Model, FilterID](setupViewQueries())

	_, err := modelView.Read(context.Background(), FilterID{
		ID: "-1",
	})

	if assert.Error(t, err) {
		assert.ErrorIs(t, err, norm.ErrNotFound)
	}
}

type FilterIDs struct {
	IDs pq.StringArray
}

func TestView_Read_Slice(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	want := []Model{
		{
			ID:        "id01",
			FieldA:    "a",
			FieldB:    "b",
			FieldC:    1234,
			CreatedAt: time.Date(2001, 9, 28, 23, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2001, 9, 28, 23, 0, 0, 0, time.UTC),
		},
		{
			ID:        "id02",
			FieldA:    "aaaa",
			FieldB:    "bbbb",
			FieldC:    4321,
			CreatedAt: time.Date(2002, 9, 28, 23, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2002, 9, 28, 23, 0, 0, 0, time.UTC),
		},
	}

	modelsView := NewView[[]Model, FilterIDs](db, `
	SELECT 
		"id", 
		"field_a",
		"field_b",
		"field_c",
		"created_at",
		"updated_at"
	FROM 
		"tests" 
	WHERE 
		"id" = ANY( {{ .A.IDs }} )
	ORDER BY 
		"id" ASC
	;`,
	)

	got, err := modelsView.Read(context.Background(), FilterIDs{
		IDs: []string{"-1", "id01", "id02"},
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := range got {
		m := &got[i]
		m.CreatedAt = m.CreatedAt.UTC()
		m.UpdatedAt = m.UpdatedAt.UTC()
	}

	assert.Equal(t, want, got)
}

func TestView_Read_Slice_EmptyResult(t *testing.T) {
	if err := resetDB(t, db); err != nil {
		t.Fatal(err)
	}

	modelsView := NewView[[]Model, FilterIDs](db, `
	SELECT 
		"id", 
		"field_a",
		"field_b",
		"field_c",
		"created_at",
		"updated_at"
	FROM 
		"tests" 
	WHERE 
		"id" = ANY( {{ .A.IDs }} )
	ORDER BY 
		"id" ASC
	;`,
	)

	got, err := modelsView.Read(context.Background(), FilterIDs{
		IDs: []string{"-1", "-2", "-3"},
	})

	assert.Len(t, got, 0)
	assert.NoError(t, err)
}
