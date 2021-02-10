package escondb

import (
	"fmt"
	"strings"
)

// SelectSQL is automatic build select sql
func SelectSQL(txtTable string, txtColumn string, txtWhere string, txtSort string, intOffset int, intLimit int) string {
	if strings.TrimSpace(txtColumn) == "" {
		txtColumn = "*"
	}

	if strings.TrimSpace(txtWhere) != "" {
		txtWhere = fmt.Sprint(" WHERE ", txtWhere)
	}

	if strings.TrimSpace(txtSort) != "" {
		txtSort = fmt.Sprint(" ORDER BY ", txtSort)
	}

	txtOffset := 0
	if intOffset >= 0 {
		txtOffset = fmt.Sprint(" OFFSET ", intOffset)
	}

	txtLimit := 0
	if intLimit >= 0 {
		txtLimit = fmt.Sprint(" LIMIT ", intLimit)
	}

	return strings.TrimSpace(fmt.Sprint("SELECT ", txtColumn, " FROM ", txtTable, txtWhere, txtSort, txtOffset, txtLimit))
}
