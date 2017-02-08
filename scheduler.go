package main

import (
	"fmt"
	"sync"
	"time"
)

const (
	limit = 100
)

type job struct {
	from   time.Time
	to     time.Time
	offset int
	limit  int
}

type scheduler struct {
	workerID int
	workers  []*fetchWorker
	mutex    sync.Mutex
}

func newScheduler(num int) *scheduler {
	sch := &scheduler{
		workers: make([]*fetchWorker, 0, num),
		mutex:   sync.Mutex{},
	}

	return sch
}

func (sch *scheduler) receiveJobs(jobs []job) {
	for _, j := range jobs {
		w := newFetchWorker(j, sch)
		sch.appendWorker(w)
	}

	for _, worker := range sch.workers {
		go worker.Run()
	}
}

func (sch *scheduler) appendWorker(w *fetchWorker) {
	sch.mutex.Lock()
	sch.workers = append(sch.workers, w)
	sch.mutex.Unlock()
}

func (sch *scheduler) printProcess() {
	start := time.Now()
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		updateStdout()
		printHeader()
		for _, w := range sch.workers {
			printWorker(w)
		}
		fmt.Println(time.Now().Sub(start))
	}
}

func printHeader() {
	fmt.Printf("ID \t Status \t Offset \t Item \t SavedItem \n")
}

func printWorker(w *fetchWorker) {
	fmt.Printf("%d \t %s \t %d \t %d \t %d \n", w.id, w.statusName(), w.currJob.offset, w.fetechedItemNum, w.savedItemNum)
}

func seperateJobs(fromDateStr, toDateStr string, jobNum int) ([]job, error) {
	fromTime, err := strToTime(fromDateStr)
	toTime, err := strToTime(toDateStr)
	if err != nil {
		return nil, err
	}

	diff := toTime.Unix() - fromTime.Unix()
	interval := int(diff) / jobNum
	if interval <= 0 {
		return nil, fmt.Errorf("jobNum is too big or fromDate is too close to toDate")
	}

	result := make([]job, 0, jobNum)
	for i := 0; i < jobNum; i++ {
		job := job{
			from:   fromTime.Add(time.Duration(i*interval) * time.Second),
			to:     fromTime.Add(time.Duration((i+1)*interval) * time.Second),
			offset: 0,
			limit:  limit,
		}

		result = append(result, job)
	}

	result[len(result)-1].to = toTime

	return result, nil
}

func strToTime(timeStr string) (time.Time, error) {
	return time.Parse("2006-01-02T15:04:05", timeStr)
}

func gotoxy(x, y int) {
	fmt.Printf("\033[%d;%dH", x, y)
}

func updateStdout() {
	//\033[0;0H
	//\033[H\033[J
	fmt.Printf("\033[H\033[J")
}
