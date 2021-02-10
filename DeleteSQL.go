package escondb

import (
	"fmt"
	"strings"
)

// DeleteSQL is automatic build delete sql
func DeleteSQL(txtTable string, txtWhere string) string {
	if strings.TrimSpace(txtWhere) != "" {
		txtWhere = fmt.Sprint(" WHERE ", txtWhere)
	}
	return strings.TrimSpace(fmt.Sprint("DELETE FROM ", txtTable, txtWhere))
}
