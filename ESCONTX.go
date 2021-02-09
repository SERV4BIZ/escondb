package escondb

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/escondb/utility"
	"github.com/SERV4BIZ/gfp/jsons"
)

// ESCONTX is transaction object
type ESCONTX struct {
	DB *ESCONDB
	TX *sql.Tx
}

// Commit is confirm save data
func (me *ESCONTX) Commit() error {
	return me.TX.Commit()
}

// Rollback is restore data to begin point
func (me *ESCONTX) Rollback() error {
	return me.TX.Rollback()
}

// Query is fetch data from query sql
func (me *ESCONTX) Query(txtSQL string) (*jsons.JSONArray, error) {
	dbRows, errRows := me.TX.Query(txtSQL)
	if errRows != nil {
		return nil, errRows
	}
	defer dbRows.Close()

	arrColTypes, errColtypes := dbRows.ColumnTypes()
	if errColtypes != nil {
		return nil, errColtypes
	}

	arrColumns, errColumns := dbRows.Columns()
	if errColumns != nil {
		return nil, errColumns
	}

	getData := make([]interface{}, len(arrColumns))
	getDataPointers := make([]interface{}, len(arrColumns))

	for index := range arrColumns {
		getDataPointers[index] = &getData[index]
	}

	jsaList := jsons.JSONArrayFactory()
	for dbRows.Next() {
		errGet := dbRows.Scan(getDataPointers...)
		if errGet != nil {
			return nil, errGet
		}
		jsaList.PutObject(me.DB.RawDataToJSONObject(arrColumns, arrColTypes, getData))
	}

	return jsaList, nil
}

// Exec is run single sql and return effect number data
func (me *ESCONTX) Exec(txtSQL string) (*jsons.JSONObject, error) {
	dbResult, errResult := me.TX.Exec(txtSQL)
	if errResult != nil {
		return nil, errResult
	}

	intInsertID, errInsertID := dbResult.LastInsertId()
	if errInsertID != nil {
		intInsertID = -1
	}

	intAffected, errAffected := dbResult.RowsAffected()
	if errAffected != nil {
		intAffected = -1
	}

	jsoItem := jsons.JSONObjectFactory()
	jsoItem.PutInt("int_insertid", int(intInsertID))
	jsoItem.PutInt("int_affected", int(intAffected))
	return jsoItem, nil
}

// Fetch is run single sql and return first data row
func (me *ESCONTX) Fetch(txtSQL string) (*jsons.JSONObject, error) {
	dbRows, errRows := me.TX.Query(txtSQL)
	if errRows != nil {
		return nil, errRows
	}
	defer dbRows.Close()

	arrColTypes, errColtypes := dbRows.ColumnTypes()
	if errColtypes != nil {
		return nil, errColtypes
	}

	arrColumns, errColumns := dbRows.Columns()
	if errColumns != nil {
		return nil, errColumns
	}

	getData := make([]interface{}, len(arrColumns))
	getDataPointers := make([]interface{}, len(arrColumns))

	for index := range arrColumns {
		getDataPointers[index] = &getData[index]
	}

	if dbRows.Next() {
		errGet := dbRows.Scan(getDataPointers...)
		if errGet != nil {
			return nil, errGet
		}
		return me.DB.RawDataToJSONObject(arrColumns, arrColTypes, getData), nil
	}

	return nil, sql.ErrNoRows
}

// FindRow is query and listing row from database
func (me *ESCONTX) FindRow(txtTableName string, jsaColumn *jsons.JSONArray, jsoCondition *jsons.JSONObject, jsoSort *jsons.JSONObject, intLimit int) (*jsons.JSONArray, error) {
	txtLimit := ""
	if intLimit > 0 {
		txtLimit = fmt.Sprint("LIMIT ", intLimit)
	}

	txtSort := ""
	if jsoSort != nil && jsoSort.Length() > 0 {
		txtSort = "ORDER BY "
		arrColumns := jsoSort.GetKeys()
		for _, columnName := range arrColumns {
			if jsoSort.GetType(columnName) == "bool" {
				if jsoSort.GetBool(columnName) {
					txtSort = fmt.Sprint(txtSort, columnName, " DESC,")
				} else {
					txtSort = fmt.Sprint(txtSort, columnName, " ASC,")
				}
			} else {
				txtSort = fmt.Sprint(txtSort, columnName, ",")
			}
		}

		txtSort = strings.TrimSpace(txtSort)
		txtSort = strings.Trim(txtSort, ",")
	}

	txtWhere := ""
	if jsoCondition != nil && jsoCondition.Length() > 0 {
		txtWhere = "WHERE "
		arrColumns := jsoCondition.GetKeys()
		for _, columnName := range arrColumns {
			switch jsoCondition.GetType(columnName) {
			case "string":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetString(columnName)), "'", " AND ")
			case "int":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetInt(columnName), " AND ")
			case "double":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetDouble(columnName), " AND ")
			case "bool":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetBool(columnName), " AND ")
			case "null":
				txtWhere = fmt.Sprint(txtWhere, columnName, " IS NULL AND ")
			case "object":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetObject(columnName).ToString()), "'", " AND ")
			case "array":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetArray(columnName).ToString()), "'", " AND ")
			}
		}

		txtWhere = strings.TrimSpace(txtWhere)
		txtWhere = strings.Trim(txtWhere, "AND")
	}

	txtColumn := "*"
	if jsaColumn != nil && jsaColumn.Length() > 0 {
		txtColumn = ""
		for i := 0; i < jsaColumn.Length(); i++ {
			txtColumn = fmt.Sprint(txtColumn, strings.TrimSpace(jsaColumn.GetString(i)), ",")
		}
		txtColumn = strings.TrimSpace(strings.Trim(txtColumn, ","))
	}

	txtSQL := fmt.Sprint("SELECT ", txtColumn, " FROM ", txtTableName, " ", txtWhere, " ", txtSort, " ", txtLimit)
	return me.Query(strings.TrimSpace(txtSQL))
}

// GetRow is query and get row from database
func (me *ESCONTX) GetRow(txtTableName string, jsaColumn *jsons.JSONArray, jsoCondition *jsons.JSONObject) (*jsons.JSONObject, error) {
	txtWhere := ""
	if jsoCondition != nil && jsoCondition.Length() > 0 {
		txtWhere = "WHERE "
		arrColumns := jsoCondition.GetKeys()
		for _, columnName := range arrColumns {
			switch jsoCondition.GetType(columnName) {
			case "string":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetString(columnName)), "'", " AND ")
			case "int":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetInt(columnName), " AND ")
			case "double":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetDouble(columnName), " AND ")
			case "bool":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetBool(columnName), " AND ")
			case "null":
				txtWhere = fmt.Sprint(txtWhere, columnName, " IS NULL AND ")
			case "object":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetObject(columnName).ToString()), "'", " AND ")
			case "array":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetArray(columnName).ToString()), "'", " AND ")
			}
		}

		txtWhere = strings.TrimSpace(txtWhere)
		txtWhere = strings.Trim(txtWhere, "AND")
	}

	txtColumn := "*"
	if jsaColumn != nil && jsaColumn.Length() > 0 {
		txtColumn = ""
		for i := 0; i < jsaColumn.Length(); i++ {
			txtColumn = fmt.Sprint(txtColumn, strings.TrimSpace(jsaColumn.GetString(i)), ",")
		}
		txtColumn = strings.TrimSpace(strings.Trim(txtColumn, ","))
	}

	txtSQL := fmt.Sprint("SELECT ", txtColumn, " FROM ", txtTableName, " ", txtWhere, " LIMIT 1")
	return me.Fetch(strings.TrimSpace(txtSQL))
}

// ExistRow is query check have any one row from condition
func (me *ESCONTX) ExistRow(txtTableName string, jsoCondition *jsons.JSONObject) error {
	var jsaColumn *jsons.JSONArray = nil
	if jsoCondition != nil && jsoCondition.Length() > 0 {
		jsaColumn = jsons.ArrayNew()
		arrColumns := jsoCondition.GetKeys()
		for _, columnName := range arrColumns {
			jsaColumn.PutString(columnName)
		}
	}
	_, err := me.GetRow(txtTableName, jsaColumn, jsoCondition)
	return err
}

// AddRow is add json object to database
func (me *ESCONTX) AddRow(txtTableName string, jsoData *jsons.JSONObject) (*jsons.JSONObject, error) {
	if jsoData == nil || jsoData.Length() == 0 {
		return nil, errors.New("Data row is empty")
	}

	arrColumns := jsoData.GetKeys()
	txtColumns := ""
	txtValues := ""
	for _, columnName := range arrColumns {
		txtColumns = fmt.Sprint(txtColumns, columnName, ",")

		switch jsoData.GetType(columnName) {
		case "string":
			txtValues = fmt.Sprint(txtValues, "'", utility.AddQuote(jsoData.GetString(columnName)), "'", ",")
		case "int":
			txtValues = fmt.Sprint(txtValues, jsoData.GetInt(columnName), ",")
		case "double":
			txtValues = fmt.Sprint(txtValues, jsoData.GetDouble(columnName), ",")
		case "bool":
			txtValues = fmt.Sprint(txtValues, jsoData.GetBool(columnName), ",")
		case "null":
			txtValues = fmt.Sprint(txtValues, columnName, " IS NULL,")
		case "object":
			txtValues = fmt.Sprint(txtValues, "'", utility.AddQuote(jsoData.GetObject(columnName).ToString()), "'", ",")
		case "array":
			txtValues = fmt.Sprint(txtValues, "'", utility.AddQuote(jsoData.GetArray(columnName).ToString()), "'", ",")
		}
	}
	txtColumns = strings.TrimSpace(strings.Trim(txtColumns, ","))
	txtValues = strings.TrimSpace(strings.Trim(txtValues, ","))

	txtSQL := fmt.Sprint("INSERT INTO ", txtTableName, " (", txtColumns, ") VALUES (", txtValues, ")")
	return me.Exec(txtSQL)
}

// DeleteRow is delete in table from database by condition and limit
func (me *ESCONTX) DeleteRow(txtTableName string, jsoCondition *jsons.JSONObject) (*jsons.JSONObject, error) {
	txtWhere := ""
	if jsoCondition != nil && jsoCondition.Length() > 0 {
		txtWhere = "WHERE "
		arrColumns := jsoCondition.GetKeys()
		for _, columnName := range arrColumns {
			switch jsoCondition.GetType(columnName) {
			case "string":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetString(columnName)), "'", " AND ")
			case "int":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetInt(columnName), " AND ")
			case "double":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetDouble(columnName), " AND ")
			case "bool":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetBool(columnName), " AND ")
			case "null":
				txtWhere = fmt.Sprint(txtWhere, columnName, " IS NULL AND ")
			case "object":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetObject(columnName).ToString()), "'", " AND ")
			case "array":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetArray(columnName).ToString()), "'", " AND ")
			}
		}

		txtWhere = strings.TrimSpace(txtWhere)
		txtWhere = strings.Trim(txtWhere, "AND")
	}

	txtSQL := fmt.Sprint("DELETE FROM ", txtTableName, " ", txtWhere)
	return me.Exec(strings.TrimSpace(txtSQL))
}

// UpdateRow is update in table from database by condition and limit
func (me *ESCONTX) UpdateRow(txtTableName string, jsoData *jsons.JSONObject, jsoCondition *jsons.JSONObject) (*jsons.JSONObject, error) {
	// set data
	if jsoData == nil || jsoData.Length() == 0 {
		return nil, errors.New("Data row is empty")
	}

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

	// find data
	txtWhere := ""
	if jsoCondition != nil && jsoCondition.Length() > 0 {
		txtWhere = "WHERE "
		arrColumns := jsoCondition.GetKeys()
		for _, columnName := range arrColumns {
			switch jsoCondition.GetType(columnName) {
			case "string":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetString(columnName)), "'", " AND ")
			case "int":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetInt(columnName), " AND ")
			case "double":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetDouble(columnName), " AND ")
			case "bool":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = ", jsoCondition.GetBool(columnName), " AND ")
			case "null":
				txtWhere = fmt.Sprint(txtWhere, columnName, " IS NULL AND ")
			case "object":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetObject(columnName).ToString()), "'", " AND ")
			case "array":
				txtWhere = fmt.Sprint(txtWhere, columnName, " = '", utility.AddQuote(jsoCondition.GetArray(columnName).ToString()), "'", " AND ")
			}
		}

		txtWhere = strings.TrimSpace(txtWhere)
		txtWhere = strings.Trim(txtWhere, "AND")
	}

	txtSQL := fmt.Sprint("UPDATE ", txtTableName, " SET ", txtSet, "  ", txtWhere)
	return me.Exec(strings.TrimSpace(txtSQL))
}
