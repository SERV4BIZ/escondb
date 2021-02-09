package escondb

import (
	"strings"
)

// AddQuote is add quota in sql in string
func AddQuote(str string) string {
	return strings.ReplaceAll(str, "'", "''")
}
