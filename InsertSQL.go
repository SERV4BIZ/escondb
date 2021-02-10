package escondb

import (
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
)

// InsertSQL is automatic build insert sql
func InsertSQL(txtTable string, jsoData *jsons.JSONObject) string {
	arrColumns := jsoData.GetKeys()
	txtColumns := ""
	txtValues := ""
	for _, columnName := range arrColumns {
		txtColumns = fmt.Sprint(txtColumns, columnName, ",")
		switch jsoData.GetType(columnName) {
		case "string":
			txtValues = fmt.Sprint(txtValues, "'", AddQuote(jsoData.GetString(columnName)), "'", ",")
		case "int":
			txtValues = fmt.Sprint(txtValues, jsoData.GetInt(columnName), ",")
		case "double":
			txtValues = fmt.Sprint(txtValues, jsoData.GetDouble(columnName), ",")
		case "bool":
			txtValues = fmt.Sprint(txtValues, jsoData.GetBool(columnName), ",")
		case "null":
			txtValues = fmt.Sprint(txtValues, columnName, " IS NULL,")
		case "object":
			txtValues = fmt.Sprint(txtValues, "'", AddQuote(jsoData.GetObject(columnName).ToString()), "'", ",")
		case "array":
			txtValues = fmt.Sprint(txtValues, "'", AddQuote(jsoData.GetArray(columnName).ToString()), "'", ",")
		}
	}
	txtColumns = strings.TrimSpace(strings.Trim(txtColumns, ","))
	txtValues = strings.TrimSpace(strings.Trim(txtValues, ","))
	return strings.TrimSpace(fmt.Sprint("INSERT INTO ", txtTable, " (", txtColumns, ") VALUES (", txtValues, ")"))
}
