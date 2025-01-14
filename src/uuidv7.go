package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/google/uuid"
	"github.com/marcboeker/go-duckdb"
)

type uuidv7Func struct{}

func uuidv7Fn(values []driver.Value) (any, error) {
	uuid, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	return uuid.String(), nil
}

func (*uuidv7Func) Config() duckdb.ScalarFuncConfig {
	uuidTypeInfo, err := duckdb.NewTypeInfo(duckdb.TYPE_UUID)
	if err != nil {
		return duckdb.ScalarFuncConfig{}
	}

	return duckdb.ScalarFuncConfig{
		ResultTypeInfo: uuidTypeInfo,
	}
}

func (*uuidv7Func) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: uuidv7Fn}
}

func registerUUIDv7UDF(db *sql.DB) error {
	c, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	var uuidv7UDF *uuidv7Func
	err = duckdb.RegisterScalarUDF(c, "uuidv7", uuidv7UDF)
	if err != nil {
		return fmt.Errorf("failed to register UUIDv7 UDF: %w", err)
	}

	return c.Close()
}
