package main

import (
	"regexp"
)

type Operation int

const (
	OpInsert Operation = iota
	OpCreateTable
	OpSelect
	OpUnknown
)

type Parser struct {
	insertRe    *regexp.Regexp
	createRe    *regexp.Regexp
	selectRe    *regexp.Regexp
}

func NewParser() *Parser {
	return &Parser{
		insertRe: regexp.MustCompile(`(?i)^\s*INSERT\s+(OR\s+(REPLACE|IGNORE)\s+)?INTO\s+([.\w]+)`),
		createRe: regexp.MustCompile(`(?i)^\s*CREATE\s+(OR\s+REPLACE\s+)?(TEMP(?:ORARY)?\s+)?TABLE\s+(\w+)`),
		selectRe: regexp.MustCompile(`(?i)^\s*SELECT\s+.+?\s+FROM\s+([.\w]+)`),
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
		return OpSelect, matches[len(matches)-1]
	}
	return OpUnknown, ""
}
