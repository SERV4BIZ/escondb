package escondb

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
)

// RawDataToJSONObject is fill data to json object
func (me *ESCONDB) RawDataToJSONObject(arrColumns []string, arrColumTypes []*sql.ColumnType, rawDatas []interface{}) *jsons.JSONObject {
	jsoItem := jsons.JSONObjectFactory()
	for i, val := range rawDatas {
		if val == nil {
			jsoItem.PutNull(arrColumns[i])
		} else {
			if me.Type == "POSTGRESQL" {
				txtType := strings.ToUpper(arrColumTypes[i].DatabaseTypeName())
				if strings.HasPrefix(txtType, "TEXT") {
					jsoItem.PutString(arrColumns[i], val.(string))
				} else if strings.HasPrefix(txtType, "NAME") {
					jsoItem.PutString(arrColumns[i], string(val.([]uint8)))
				} else if strings.HasPrefix(txtType, "CHAR") {
					jsoItem.PutString(arrColumns[i], val.(string))
				} else if strings.HasPrefix(txtType, "INT") {
					jsoItem.PutInt(arrColumns[i], int(val.(int64)))
				} else if strings.HasPrefix(txtType, "FLOAT") {
					jsoItem.PutFloat(arrColumns[i], val.(float64))
				} else if strings.HasPrefix(txtType, "BOOL") {
					jsoItem.PutBool(arrColumns[i], val.(bool))
				} else if strings.HasPrefix(txtType, "JSON") {
					txtBuffer := strings.TrimSpace(string(val.([]uint8)))
					if strings.HasPrefix(txtBuffer, "{") {
						jsoNewObj, errNewObj := jsons.JSONObjectFromString(txtBuffer)
						if errNewObj == nil {
							jsoItem.PutObject(arrColumns[i], jsoNewObj)
						}
					} else if strings.HasPrefix(txtBuffer, "[") {
						jsoNewArr, errNewArr := jsons.JSONArrayFromString(txtBuffer)
						if errNewArr == nil {
							jsoItem.PutArray(arrColumns[i], jsoNewArr)
						}
					}
				} else if strings.HasPrefix(txtType, "DATE") {
					jsoItem.PutString(arrColumns[i], fmt.Sprint(val))
				} else if strings.HasPrefix(txtType, "TIME") {
					jsoItem.PutString(arrColumns[i], fmt.Sprint(val))
				}
			}
		}
	}
	return jsoItem
}
