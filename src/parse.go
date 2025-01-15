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

type ColumnDef struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"primary_key,omitempty"`
	Default    string `json:"default,omitempty"`
}

type PrimaryKeyDef struct {
	Columns []string `json:"columns"`
}

type TableDefinition struct {
	Name    string     `json:"name"`
	Columns []ColumnDef `json:"columns"`
	Primary *PrimaryKeyDef `json:"primary_key,omitempty"`
}

func LogWalkSQL(sql string, logWalk bool) (*TableDefinition, error) {
	var tableDef *TableDefinition
	w := &walk.AstWalker{
		Fn: func(ctx interface{}, node interface{}) (stop bool) {
			switch n := node.(type) {
			case *tree.CreateTable:
				// Initialize table definition
				tableDef = &TableDefinition{
					Name:    n.Table.Table(),
					Columns: []ColumnDef{},
				}

				if logWalk {
					log.Printf("CREATE TABLE: %s", tableDef.Name)
				}

				// Process table-level primary key
				for _, def := range n.Defs {
					if pk, ok := def.(*tree.UniqueConstraintTableDef); ok && pk.PrimaryKey {
						var cols []string
						for _, col := range pk.Columns {
							cols = append(cols, col.Column.String())
						}
						tableDef.Primary = &PrimaryKeyDef{Columns: cols}

						if logWalk {
							log.Printf("  PRIMARY KEY: (%s)", strings.Join(cols, ", "))
						}
					}
				}

			case *tree.ColumnTableDef:
				if tableDef == nil {
					return false
				}

				colDef := ColumnDef{
					Name:       n.Name.String(),
					Type:       n.Type.String(),
					PrimaryKey: n.PrimaryKey.IsPrimaryKey,
				}

				// Process default value
				if n.DefaultExpr.Expr != nil {
					colDef.Default = n.DefaultExpr.Expr.String()
				}

				// Add to table definition
				tableDef.Columns = append(tableDef.Columns, colDef)

				if logWalk {
					log.Printf("  COLUMN: %s %s", colDef.Name, colDef.Type)
					if colDef.Default != "" {
						log.Printf("    DEFAULT: %s", colDef.Default)
					}
					if colDef.PrimaryKey {
						log.Printf("    PRIMARY KEY")
					}
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
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	_, err = w.Walk(stmts, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to walk AST: %w", err)
	}

	// Return nil for non-CREATE TABLE statements
	if tableDef == nil {
		return nil, nil
	}

	return tableDef, nil
}
