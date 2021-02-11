package escondb

import (
	"strings"
)

// Quote is add quota in sql in string
func Quote(str string) string {
	return "'" + strings.ReplaceAll(str, "'", "''") + "'"
}
