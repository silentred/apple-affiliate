package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorker(t *testing.T) {
	InitDB("127.0.0.1:3306", "jason", "jason", "fenda")

	end := time.Now().UTC()
	start := end.Add(-60 * time.Minute)
	fmt.Println(start)
	fmt.Println(end)

	j := &job{
		from:   start,
		to:     end,
		offset: 0,
		limit:  10,
	}

	worker := fetchWorker{
		id:      1,
		status:  statusRunning,
		currJob: j,
	}

	err, hasNext := worker.DoJob()
	assert.NoError(t, err)
	assert.Equal(t, true, hasNext)
}
