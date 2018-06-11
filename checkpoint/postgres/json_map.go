package postgres

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

// JSONMap used to handle json b on postgres, It implements Valuer and Scanner interfaces
type JSONMap map[string]interface{}

// Value returns the json serialization of PropertyMap
func (p JSONMap) Value() (driver.Value, error) {
	j, err := json.Marshal(p)
	return j, err
}

// Scan deserializes the json
func (p *JSONMap) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return errors.New("Type assertion []byte failed")
	}

	var i interface{}
	err := json.Unmarshal(source, &i)
	if err != nil {
		return err
	}

	*p, ok = i.(map[string]interface{})
	if !ok {
		return errors.New("Type assertion map[string]interface{} failed")
	}

	return nil
}
