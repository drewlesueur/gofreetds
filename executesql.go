package freetds

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"time"
	"log"
	"database/sql"
)

const statusRow string = `;
   select cast(coalesce(scope_identity(), -1) as bigint) last_insert_id, 
          cast(@@rowcount as bigint) rows_affected
`
const statusRowSybase125 string = `
   select cast(coalesce(@@IDENTITY, -1) as int) last_insert_id, 
          cast(@@rowcount as int) rows_affected
`

//Execute sql query with arguments.
//? in query are arguments placeholders.
//  ExecuteSql("select * from authors where au_fname = ?", "John")
func (conn *Conn) ExecuteSql(query string, params ...driver.Value) ([]*Result, error) {
	if conn.sybaseMode125() {
		return conn.executeSqlSybase125(query, params...)
	}
	statement, numParams := query2Statement(query)
	if numParams != len(params) {
		return nil, fmt.Errorf("Incorrect number of params, expecting %d got %d", numParams, len(params))
	}
	paramDef, paramVal, err := parseParams(params...)
	if err != nil {
		return nil, err
	}

	log.Printf("========paramDef: %s", paramDef)
	log.Printf("========paramVal: %s", paramVal)

	statement += statusRow

	sql := fmt.Sprintf("exec sp_executesql N'%s', N'%s', %s", statement, paramDef, paramVal)

	if numParams == 0 {
		sql = fmt.Sprintf("exec sp_executesql N'%s'", statement)
	}
	log.Printf("=====full sql: %s", sql)
	return conn.Exec(sql)
}

func (conn *Conn) executeSqlSybase125(query string, params ...driver.Value) ([]*Result, error) {
	statement, numParams := query2Statement(query)
	if numParams != len(params) {
		return nil, fmt.Errorf("Incorrect number of params, expecting %d got %d", numParams, len(params))
	}

	statement += statusRowSybase125
	sql := strings.Replace(query, "?", "$bindkey", -1)
	re, _ := regexp.Compile(`(?P<bindkey>\$bindkey)`)
	matches := re.FindAllSubmatchIndex([]byte(sql), -1)

	for i, _ := range matches {
		_, escapedValue, _ := go2SqlDataType(params[i])
		sql = fmt.Sprintf("%s", strings.Replace(sql, "$bindkey", escapedValue, 1))
	}

	if numParams == 0 {
		sql = fmt.Sprintf("%s", statement)
	}
	return conn.Exec(sql)
}

//converts query to SqlServer statement for sp_executesql
//replaces ? in query with params @p1, @p2, ...
//returns statement and number of params
func query2Statement(query string) (string, int) {
	parts := strings.Split(query, "?")
	var statement string
	numParams := len(parts) - 1
	statement = parts[0]
	for i, part := range parts {
		if i > 0 {
			statement = fmt.Sprintf("%s@p%d%s", statement, i, part)
		}
	}
	return quote(statement), numParams
}

func parseParams(params ...driver.Value) (string, string, error) {
	paramDef := ""
	paramVal := ""
	for i, param := range params {
		if i > 0 {
			paramVal += ", "
			paramDef += ", "
		}
		sqlType, sqlValue, err := go2SqlDataType(param)
		if err != nil {
			return "", "", err
		}
		paramName := fmt.Sprintf("@p%d", i+1)
		paramDef += fmt.Sprintf("%s %s", paramName, sqlType)
		paramVal += fmt.Sprintf("%s=%s", paramName, sqlValue)
	}
	return paramDef, paramVal, nil
}

func quote(in string) string {
	return strings.Replace(in, "'", "''", -1)
}

func go2SqlDataType(value interface{}) (string, string, error) {
	max := func(a int, b int) int {
		if a > b {
			return a
		}
		return b
	}

	isSQLType := true
	switch t := value.(type) {
	case sql.NullBool:
		if t.Valid {
			value = t.Bool	
			break
		} else {
			return "bit", "NULL", nil
		}
	case sql.NullFloat64:
		if t.Valid {
			value = t.Float64	
			break
		} else {
			return "real", "NULL", nil
		}
	case sql.NullInt32:
		if t.Valid {
			value = t.Int32	
			break
		} else {
			return "int", "NULL", nil
		}
	case sql.NullInt64:
		if t.Valid {
			value = t.Int64
			break
		} else {
			return "bigint", "NULL", nil
		}
	case sql.NullString:
		if t.Valid {
			value = t.String
			break
		} else {
			return "nvarchar (1)", "NULL", nil
		}
	case sql.NullTime:
		if t.Valid {
			value = t.Time
			break
		} else {
			return "datetimeoffset", "NULL", nil
		}
	default:
		isSQLType = false	
	}

	if !isSQLType {
		switch t := value.(type) {
		case *bool:
			if t == nil {
				return "bit", "NULL", nil	
			} else {
				value = *t	
			}
		case *uint8:
			if t == nil {
				return "tinyint", "NULL", nil	
			} else {
				value = *t	
			}
		case *int8:
			if t == nil {
				return "tinyint", "NULL", nil	
			} else {
				value = *t	
			}
		case *uint16:
			if t == nil {
				return "smallint", "NULL", nil	
			} else {
				value = *t	
			}
		case *int16:
			if t == nil {
				return "smallint", "NULL", nil	
			} else {
				value = *t	
			}
		case *uint32:
			if t == nil {
				return "int", "NULL", nil	
			} else {
				value = *t	
			}
		case *int32:
			if t == nil {
				return "int", "NULL", nil	
			} else {
				value = *t	
			}
		case *int:
			if t == nil {
				return "int", "NULL", nil	
			} else {
				value = *t	
			}
		case *uint64:
			if t == nil {
				return "bigint", "NULL", nil	
			} else {
				value = *t	
			}
		case *int64:
			if t == nil {
				return "bigint", "NULL", nil	
			} else {
				value = *t	
			}
		case *float32:
			if t == nil {
				return "real", "NULL", nil	
			} else {
				value = *t	
			}
		case *float64:
			if t == nil {
				return "real", "NULL", nil	
			} else {
				value = *t	
			}
		case *string:
			if t == nil {
				return "nvarchar (1)", "NULL", nil	
			} else {
				value = *t	
			}
		case *time.Time:
			if t == nil {
				return "datetimeoffset", "NULL", nil	
			} else {
				value = *t	
			}
		case *[]byte:
			if t == nil {
				return "varbinary (1)", "NULL", nil	
			} else {
				value = *t	
			}
		}
	
	}

	strValue := fmt.Sprintf("%v", value)
	switch t := value.(type) {
	case nil:
		return "nvarchar (1)", "NULL", nil
	case bool:
		bitStrValue := "0"
		if strValue == "true" {
			bitStrValue = "1"
		}
		return "bit", bitStrValue, nil
	case uint8, int8:
		return "tinyint", strValue, nil
	case uint16, int16:
		return "smallint", strValue, nil
	case uint32, int32, int:
		return "int", strValue, nil
	case uint64, int64:
		return "bigint", strValue, nil
	case float32, float64:
		return "real", strValue, nil
	case string:
		{
		}
	case time.Time:
		{
			strValue = t.Format(time.RFC3339Nano)
			return "datetimeoffset", fmt.Sprintf("'%s'", quote(strValue)), nil
		}
	case []byte:
		{
			b, _ := value.([]byte)
			return fmt.Sprintf("varbinary (%d)", max(1, len(b))),
				fmt.Sprintf("0x%x", b), nil
		}
	default:
		return "", "", fmt.Errorf("unknown dataType %T", t)
	}
	return fmt.Sprintf("nvarchar (%d)", max(1, len(strValue))),
		fmt.Sprintf("'%s'", quote(strValue)), nil

}
