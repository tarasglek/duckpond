package main

import (
	"regexp"
)

type Operation int

const (
	OpInsert Operation = iota
	OpCreateTable
	OpSelect
	OpAlterTable
	OpVacuum
	OpDropTable
	OpUnknown
)

func (o Operation) String() string {
	switch o {
	case OpInsert:
		return "insert"
	case OpCreateTable:
		return "create_table"
	case OpSelect:
		return "select"
	case OpAlterTable:
		return "alter_table"
	case OpVacuum:
		return "vacuum"
	case OpDropTable:
		return "drop_table"
	default:
		return "unknown"
	}
}

type Parser struct {
	insertRe *regexp.Regexp
	createRe *regexp.Regexp
	selectRe *regexp.Regexp
	alterRe  *regexp.Regexp
	vacuumRe *regexp.Regexp
	dropRe   *regexp.Regexp
}

func NewParser() *Parser {
	return &Parser{
		insertRe: regexp.MustCompile(`(?i)^\s*INSERT\s+(OR\s+(REPLACE|IGNORE)\s+)?INTO\s+([.\w]+)`),
		createRe: regexp.MustCompile(`(?i)^\s*CREATE\s+(OR\s+REPLACE\s+)?(TEMP(?:ORARY)?\s+)?TABLE\s+(\w+)`),
		selectRe: regexp.MustCompile(`(?i)^\s*SELECT\s+.*?(?:\s+FROM\s+([.\w]+))?[\s;]*$`),
		alterRe:  regexp.MustCompile(`(?i)^\s*ALTER\s+TABLE\s+([.\w]+)`),
		vacuumRe: regexp.MustCompile(`(?i)^\s*VACUUM(?:\s+(\S+))?`),
		dropRe:   regexp.MustCompile(`(?i)^\s*DROP\s+TABLE\s+([.\w]+)`),
	}
}

func (p *Parser) Parse(query string) (Operation, string) {
	if matches := p.insertRe.FindStringSubmatch(query); matches != nil {
		return OpInsert, matches[len(matches)-1]
	}
	if matches := p.createRe.FindStringSubmatch(query); matches != nil {
		return OpCreateTable, matches[len(matches)-1]
	}
	if matches := p.selectRe.FindStringSubmatch(query); matches != nil {
		// If no table found, return empty string
		if len(matches) > 1 && matches[1] != "" {
			return OpSelect, matches[1]
		}
		return OpSelect, ""
	}
	if matches := p.alterRe.FindStringSubmatch(query); matches != nil {
		return OpAlterTable, matches[len(matches)-1]
	}
	if matches := p.vacuumRe.FindStringSubmatch(query); matches != nil {
		table := ""
		if len(matches) > 1 {
			table = matches[1]
		}
		return OpVacuum, table
	}
	if matches := p.dropRe.FindStringSubmatch(query); matches != nil {
		return OpDropTable, matches[1]
	}
	return OpUnknown, ""
}
