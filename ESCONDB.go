package escondb

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/escondb/utility"
	"github.com/SERV4BIZ/gfp/jsons"
)

// ESCONDB is connection object
type ESCONDB struct {
	DB *sql.DB

	Type     string
	Host     string
	Port     int
	Username string
	Password string
	Database string
}

// Ping is test connection
func (me *ESCONDB) Ping() error {
	return me.DB.Ping()
}

// Close is close connection
func (me *ESCONDB) Close() error {
	return me.DB.Close()
}

// Begin is create tx object
func (me *ESCONDB) Begin() (*ESCONTX, error) {
	tx, err := me.DB.Begin()
	if err != nil {
		return nil, err
	}

	contx := new(ESCONTX)
	contx.DB = me
	contx.TX = tx
	return contx, nil
}

// Query is fetch data from query sql
func (me *ESCONDB) Query(txtSQL string) (*jsons.JSONArray, error) {
	dbRows, errRows := me.DB.Query(txtSQL)
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
		jsaList.PutObject(me.RawDataToJSONObject(arrColumns, arrColTypes, getData))
	}

	return jsaList, nil
}

// Exec is run single sql and return effect number data
func (me *ESCONDB) Exec(txtSQL string) (*jsons.JSONObject, error) {
	dbResult, errResult := me.DB.Exec(txtSQL)
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
func (me *ESCONDB) Fetch(txtSQL string) (*jsons.JSONObject, error) {
	dbRows, errRows := me.DB.Query(txtSQL)
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
		return me.RawDataToJSONObject(arrColumns, arrColTypes, getData), nil
	}

	return nil, sql.ErrNoRows
}

// FindRow is query and listing row from database
func (me *ESCONDB) FindRow(txtTableName, jsaColumn *jsons.JSONArray, jsoCondition *jsons.JSONObject, intLimit int) (*jsons.JSONArray, error) {
	txtLimit := ""
	if intLimit > 0 {
		txtLimit = fmt.Sprint("LIMIT ", intLimit)
	}

	txtWhere := ""
	if jsoCondition.Length() > 0 {
		txtWhere = "WHERE"
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
	if jsaColumn.Length() > 0 {
		txtColumn = ""
		for i := 0; i < jsaColumn.Length(); i++ {
			txtColumn = strings.TrimSpace(strings.Trim(jsaColumn.GetString(i), ","))
		}
		txtColumn = strings.TrimSpace(strings.Trim(txtColumn, ","))
	}

	txtSQL := fmt.Sprint("SELECT ", txtColumn, " FROM ", txtTableName, " ", txtWhere, " ", txtLimit)
	return me.Query(strings.TrimSpace(txtSQL))
}

// GetRow is query and get row from database
func (me *ESCONDB) GetRow(txtTableName, jsaColumn *jsons.JSONArray, jsoCondition *jsons.JSONObject) (*jsons.JSONObject, error) {
	txtWhere := ""
	if jsoCondition.Length() > 0 {
		txtWhere = "WHERE"
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
	if jsaColumn.Length() > 0 {
		txtColumn = ""
		for i := 0; i < jsaColumn.Length(); i++ {
			txtColumn = strings.TrimSpace(strings.Trim(jsaColumn.GetString(i), ","))
		}
		txtColumn = strings.TrimSpace(strings.Trim(txtColumn, ","))
	}

	txtSQL := fmt.Sprint("SELECT ", txtColumn, " FROM ", txtTableName, " ", txtWhere, " LIMIT 1")
	return me.Fetch(strings.TrimSpace(txtSQL))
}

// AddRow is add json object to database
func (me *ESCONDB) AddRow(txtTableName, jsoData *jsons.JSONObject) (*jsons.JSONObject, error) {
	if jsoData.Length() == 0 {
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
func (me *ESCONDB) DeleteRow(txtTableName, jsoCondition *jsons.JSONObject, intLimit int) (*jsons.JSONObject, error) {

	txtLimit := ""
	if intLimit > 0 {
		txtLimit = fmt.Sprint("LIMIT ", intLimit)
	}

	txtWhere := ""
	if jsoCondition.Length() > 0 {
		txtWhere = "WHERE"
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

	txtSQL := fmt.Sprint("DELETE FROM ", txtTableName, " ", txtWhere, " ", txtLimit)
	return me.Exec(strings.TrimSpace(txtSQL))
}
