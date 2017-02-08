package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTableName(t *testing.T) {
	c := conversion{}
	c.ConversionTime = time.Now()
	fmt.Println(c.TableName())
}

func TestInsertConversion(t *testing.T) {
	InitDB("127.0.0.1:3306", "jason", "jason", "fenda")
	// c := conversionRaw{
	// 	ConversionID: "sdf",
	// }

	// c := conversion{}
	// c.ConversionID = "sdf123"
	// c.ConversionTime = time.Now()
	// c.CreatedAt = time.Now()
	// c.UpdatedAt = time.Now()

	// err := c.insert()
	// assert.NoError(t, err)

	// c, err := findByConversionID(time.Now(), "no exists")
	// assert.Equal(t, orm.ErrNoRows, err)
	// assert.Nil(t, c)

	c := conversion{}
	c.ID = 43430
	c.ConversionTime = time.Now()

	err := c.update("approved", 19.99)
	assert.NoError(t, err)

}
