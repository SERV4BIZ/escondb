package escondb

import (
	"database/sql"

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

// Exist is query check have any one row
func (me *ESCONTX) Exist(txtSQL string) error {
	_, err := me.Fetch(txtSQL)
	return err
}
