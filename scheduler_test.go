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
	fromTime, _ := strToTimeNoT("2017-02-13 00:00:00")
	toTime, _ := strToTimeNoT("2017-02-13 23:59:59")
	jobs, err := seperateJobs(fromTime, toTime, 4)
	assert.NoError(t, err)
	for _, val := range jobs {
		from := val.from.Format("2006-01-02T15:04:05")
		to := val.to.Format("2006-01-02T15:04:05")
		fmt.Printf("%s - %s \n", from, to)
	}
}
