package escondb

import (
	"fmt"
	"strings"

	"github.com/SERV4BIZ/escondb/utility"
	"github.com/SERV4BIZ/gfp/jsons"
)

// UpdateSQL is automatic build update sql
func UpdateSQL(txtTable string, jsoData *jsons.JSONObject, txtWhere string) string {
	txtSet := ""
	arrColumns := jsoData.GetKeys()
	for _, columnName := range arrColumns {
		switch jsoData.GetType(columnName) {
		case "string":
			txtSet = fmt.Sprint(txtSet, columnName, " = '", utility.AddQuote(jsoData.GetString(columnName)), "',")
		case "int":
			txtSet = fmt.Sprint(txtSet, columnName, " = ", jsoData.GetInt(columnName), ",")
		case "double":
			txtSet = fmt.Sprint(txtSet, columnName, " = ", jsoData.GetDouble(columnName), ",")
		case "bool":
			txtSet = fmt.Sprint(txtSet, columnName, " = ", jsoData.GetBool(columnName), ",")
		case "null":
			txtSet = fmt.Sprint(txtSet, columnName, " = NULL,")
		case "object":
			txtSet = fmt.Sprint(txtSet, columnName, " = '", utility.AddQuote(jsoData.GetObject(columnName).ToString()), "',")
		case "array":
			txtSet = fmt.Sprint(txtSet, columnName, " = '", utility.AddQuote(jsoData.GetArray(columnName).ToString()), "',")
		}
	}
	txtSet = strings.TrimSpace(txtSet)
	txtSet = strings.Trim(txtSet, ",")

	if strings.TrimSpace(txtWhere) != "" {
		txtWhere = fmt.Sprint(" WHERE ", txtWhere)
	}

	return strings.TrimSpace(fmt.Sprint("UPDATE ", txtTable, " SET ", txtSet, txtWhere))
}
