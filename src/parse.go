package main

import (
	"fmt"
	"log"
	"strings"

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
				
				// Print primary key constraints if they exist
				for _, def := range n.Defs {
					if pk, ok := def.(*tree.UniqueConstraintTableDef); ok && pk.PrimaryKey {
						var cols []string
						for _, col := range pk.Columns {
							cols = append(cols, col.Column.String())
						}
						log.Printf("  PRIMARY KEY: (%s)", strings.Join(cols, ", "))
					}
				}
				
			case *tree.ColumnTableDef:
				// Print column name, type, and constraints
				log.Printf("  COLUMN: %s %s", n.Name, n.Type)
				
				// Print default value if exists
				if n.DefaultExpr.Expr != nil {
					log.Printf("    DEFAULT: %s", n.DefaultExpr.Expr)
				}
				
				// Print if column is primary key by checking constraints
				if n.PrimaryKey.IsPrimaryKey {
					log.Printf("    PRIMARY KEY")
				}
				
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
