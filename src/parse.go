package main

import (
	"fmt"
	"log"

	// these deps are absolutely gigantic
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/auxten/postgresql-parser/pkg/walk"
)

func LogWalkSQL(sql string) error {
	w := &walk.AstWalker{
		Fn: func(ctx interface{}, node interface{}) (stop bool) {
			switch n := node.(type) {
			case *tree.CreateTable:
				// Print table name for CREATE TABLE statements
				log.Printf("CREATE TABLE: %s", n.Table.Table())
			case *tree.ColumnTableDef:
				// Print column name for column definitions
				log.Printf("  COLUMN: %s", n.Name)
			default:
				// Default case for all other node types
				log.Printf("node type %T", node)
			}
			return false
		},
	}

	stmts, err := parser.Parse(sql)
	if err != nil {
		return fmt.Errorf("failed to parse SQL: %w", err)
	}

	_, err = w.Walk(stmts, nil)
	if err != nil {
		return fmt.Errorf("failed to walk AST: %w", err)
	}

	return nil
}
