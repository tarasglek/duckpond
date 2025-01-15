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

type uuidV7TimeFunc struct{}

func uuidV7TimeFn(values []driver.Value) (any, error) {
	// Validate input
	if len(values) != 1 {
		return nil, fmt.Errorf("uuid_v7_time expects exactly 1 argument")
	}

	// Only accept []byte input
	uuidBytes, ok := values[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("uuid_v7_time requires UUID in byte format, got %T", values[0])
	}

	// Parse UUID
	uuid, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID: %w", err)
	}

	// Extract timestamp from UUIDv7
	if uuid.Version() != 7 {
		return nil, fmt.Errorf("UUID is not version 7")
	}

	// First 48 bits are the timestamp
	timestamp := int64(uuid[0])<<40 | int64(uuid[1])<<32 | int64(uuid[2])<<24 |
		int64(uuid[3])<<16 | int64(uuid[4])<<8 | int64(uuid[5])

	return timestamp, nil
}

func (*uuidV7TimeFunc) Config() duckdb.ScalarFuncConfig {
	bigintType, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	if err != nil {
		panic(fmt.Sprintf("failed to create BIGINT type info: %v", err))
	}

	return duckdb.ScalarFuncConfig{
		ResultTypeInfo: bigintType,
	}
}

func (*uuidV7TimeFunc) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: uuidV7TimeFn}
}

func uuidv7Fn(values []driver.Value) (any, error) {
	uuid, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	// Return as byte slice instead of string
	return uuid[:], nil
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

func registerUUIDv7TimeUDF(db *sql.DB) error {
	c, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	var uuidV7TimeUDF *uuidV7TimeFunc
	err = duckdb.RegisterScalarUDF(c, "uuid_v7_time", uuidV7TimeUDF)
	if err != nil {
		return fmt.Errorf("failed to register uuid_v7_time UDF: %w", err)
	}

	return c.Close()
}
