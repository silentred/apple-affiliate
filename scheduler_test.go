package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTime(t *testing.T) {
	time, err := strToTime("2017-01-22T12:23:44")
	assert.NoError(t, err)
	fmt.Println(time)
}

func TestJobs(t *testing.T) {
	jobs, err := seperateJobs("2017-02-06T1:00:00", "2017-02-07T01:00:00", 6)
	assert.NoError(t, err)
	for _, val := range jobs {
		from := val.from.Format("2006-01-02T15:04:05")
		to := val.to.Format("2006-01-02T15:04:05")
		fmt.Printf("%s - %s \n", from, to)
	}
}
