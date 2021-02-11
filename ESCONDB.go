package escondb

import (
	"database/sql"

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

// Exist is query check have any one row
func (me *ESCONDB) Exist(txtSQL string) error {
	_, err := me.Fetch(txtSQL)
	return err
}

// SelectRow is select data row from database
func (me *ESCONDB) SelectRow(txtTable string, txtColumn string, txtWhere string, txtSort string, intOffset int, intLimit int) (*jsons.JSONArray, error) {
	txtSQL := SelectSQL(txtTable, txtColumn, txtWhere, txtSort, intOffset, intLimit)
	return me.Query(txtSQL)
}

// InsertRow is insert data data to database
func (me *ESCONDB) InsertRow(txtTable string, jsoData *jsons.JSONObject) (*jsons.JSONObject, error) {
	txtSQL := InsertSQL(txtTable, jsoData)
	return me.Exec(txtSQL)
}

// UpdateRow is updata data in database
func (me *ESCONDB) UpdateRow(txtTable string, jsoData *jsons.JSONObject, txtWhere string) (*jsons.JSONObject, error) {
	txtSQL := UpdateSQL(txtTable, jsoData, txtWhere)
	return me.Exec(txtSQL)
}

// DeleteRow is delete data in database
func (me *ESCONDB) DeleteRow(txtTable string, txtWhere string) (*jsons.JSONObject, error) {
	txtSQL := DeleteSQL(txtTable, txtWhere)
	return me.Exec(txtSQL)
}

// FetchRow is get first select data row from database
func (me *ESCONDB) FetchRow(txtTable string, txtColumn string, txtWhere string) (*jsons.JSONObject, error) {
	txtSQL := SelectSQL(txtTable, txtColumn, txtWhere, "", -1, 1)
	return me.Fetch(txtSQL)
}

// ExistRow is check data row from database
func (me *ESCONDB) ExistRow(txtTable string, txtWhere string) error {
	_, err := FetchRow(txtTable, "*", txtWhere)
	return err
}
