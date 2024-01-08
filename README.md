# NORM

![test](https://github.com/WinPooh32/norm/actions/workflows/test.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/WinPooh32/norm)](https://goreportcard.com/report/github.com/WinPooh32/norm)
[![Go Reference](https://pkg.go.dev/badge/github.com/WinPooh32/norm.svg)](https://pkg.go.dev/github.com/WinPooh32/norm)

NORM is **N**ot an **ORM**.

It does:

- provide **unified** CRUD interfaces for your queries;
- use generics for your types;
- **not** generate SQL migrations;
- **not** generate queries from structs;
- **not** manage transactions.

## Drivers

- [SQL](https://pkg.go.dev/github.com/WinPooh32/norm/driver/sql)
- MongoDB (TODO)

## Examples

### SQL

#### View of the model list

```go
package main

import (
    "context"
    "database/sql"
    
    "github.com/WinPooh32/norm"
    normsql "github.com/WinPooh32/norm/driver/sql"
    "github.com/lib/pq"
)

type Model struct {
    ID        string    `db:"id"`
    FieldA    string    `db:"field_a"`
    FieldB    string    `db:"field_b"`
    FieldC    int       `db:"field_c"`
    CreatedAt time.Time `db:"created_at"`
    UpdatedAt time.Time `db:"updated_at"`
}

type ArgIDs struct {
    IDs pq.StringArray
}

var db *sql.DB

var modelsView norm.View[[]Model, ArgIDs]

func init(){
    // Connect to the database.
    db = ...

    modelsView = normsql.NewView[[]Model, ArgIDs](db, `
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
}

func main(){
    values, _ := modelsView.Read(context.Background(), ArgsIDs{
        IDs: []string{"id01", "id02"},
    })

    fmt.Println(values)
}

```
