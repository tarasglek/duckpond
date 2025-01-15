package main

import (
	"fmt"
	"log"

	// these deps are absolutely gigantic
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/walk"
)

func ParseSQL(sql string) error {
	w := &walk.AstWalker{
		Fn: func(ctx interface{}, node interface{}) (stop bool) {
			log.Printf("node type %T", node)
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
