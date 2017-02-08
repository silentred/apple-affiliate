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

func (j job) String() string {
	return fmt.Sprintf("-from=%s -to=%s -offset=%d", j.from.Format("2006-01-02T15:04:05"), j.to.Format("2006-01-02T15:04:05"), j.offset)
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

func (sch *scheduler) rescheduleJob() error {
	var stoppedWorker, runningWorker *fetchWorker

	for _, w := range sch.workers {
		if w.status == statusStop {
			stoppedWorker = w
		}

		if runningWorker == nil && longerThan(w.lastConvTime, w.currJob.to, time.Hour) && (w.status == statusRunning) {
			runningWorker = w
		}
	}

	if runningWorker != nil && stoppedWorker != nil {
		jobs, err := seperateJobs(runningWorker.lastConvTime, runningWorker.currJob.to, 2)
		if err != nil {
			return err
		}
		if len(jobs) == 2 {
			stoppedWorker.currJob = jobs[0]
			runningWorker.currJob = jobs[1]
			go stoppedWorker.Run()
		} else {
			return fmt.Errorf("jobs length is not 2; %#v", jobs)
		}
	}

	return nil
}

func (sch *scheduler) printProcess() {
	var allStop bool
	start := time.Now()
	ticker := time.NewTicker(time.Second)

	for range ticker.C {
		updateStdout()
		printHeader()
		for _, w := range sch.workers {
			printWorker(w)
			allStop = true
			allStop = allStop && (w.status == statusStop)
		}
		fmt.Println(time.Now().Sub(start))

		if allStop {
			break
		}
	}
}

func printHeader() {
	fmt.Printf("ID \t Status \t Offset \t Item \t SavedItem \t Range \n")
}

func printWorker(w *fetchWorker) {
	fmt.Printf("%d \t %s \t %d \t %d \t %d \t %s \n", w.id, w.statusName(), w.currJob.offset, w.fetechedItemNum, w.savedItemNum, w.currJob.String())
}

func seperateJobs(fromTime, toTime time.Time, jobNum int) ([]job, error) {
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

func longerThan(from, to time.Time, d time.Duration) bool {
	return to.Sub(from) > d
}
