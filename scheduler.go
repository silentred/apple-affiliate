package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/gizak/termui"
	"github.com/golang/glog"
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

func (sch *scheduler) createWorker(workerNum int) {
	for i := 0; i < workerNum; i++ {
		w := newFetchWorker(job{}, sch)
		sch.appendWorker(w)
	}
}

func (sch *scheduler) receiveJobs(jobs []job) {
	if len(jobs) > len(sch.workers) {
		glog.Error("jobs.length is larger sch.workers")
		return
	}

	for i := 0; i < len(sch.workers); i++ {
		sch.workers[i].currJob = jobs[i]
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

	sch.mutex.Lock()
	defer sch.mutex.Unlock()

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

func (sch *scheduler) totalProcess(info *fetchInfo) {
	var offset, fetched, saved, stopWorkerNum, workerNum int
	for _, w := range sch.workers {
		workerNum++
		offset += w.currJob.offset
		fetched += w.fetechedItemNum
		saved += w.savedItemNum
		stopWorkerNum += w.status
	}
	info.Offset = offset
	info.FetchedNum = fetched
	info.SavedNum = saved
	info.StopNum = stopWorkerNum
	info.WorkerNum = workerNum
}

func (sch *scheduler) printProcess() {
	var allStop bool
	var totalSavedItemNum, totalItemNum int

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

	for _, w := range sch.workers {
		totalItemNum += w.fetechedItemNum
		totalSavedItemNum += w.savedItemNum
	}

	fmt.Printf("total: %d , total save: %d \n", totalItemNum, totalSavedItemNum)
}

func (sch *scheduler) printProcessWithUI() {
	var allStop bool
	var totalSavedItemNum, totalItemNum int

	//start := time.Now()
	//ticker := time.NewTicker(time.Second)

	err := termui.Init()
	if err != nil {
		panic(err)
	}
	defer termui.Close()

	// top bar
	header := termui.NewPar("Press q to quit")
	header.Height = 1
	header.Width = 50
	header.Border = false
	header.TextBgColor = termui.ColorBlue
	termui.Render(header)

	//fmt.Printf("ID \t Status \t Offset \t Item \t SavedItem \t Range \n")
	tableHeader := []string{"ID", "Status", "Offset", "Item", "SavedItem", "Range"}
	table1 := termui.NewTable()
	table1.FgColor = termui.ColorWhite
	table1.BgColor = termui.ColorDefault
	table1.Y = 1
	table1.X = 0

	// press q to quit
	termui.Handle("/sys/kbd/q", func(termui.Event) {
		termui.StopLoop()
	})

	termui.Handle("/timer/1s", func(e termui.Event) {
		rows := make([][]string, 0, 10)
		rows = append(rows, tableHeader)
		for _, w := range sch.workers {
			rows = append(rows, w.getStatus())
			allStop = true
			allStop = allStop && (w.status == statusStop)
		}

		table1.Rows = rows
		table1.Analysis()
		table1.SetSize()
		termui.Render(table1)
		//fmt.Println(time.Now().Sub(start))
		if allStop {
			return
		}
	})

	// for range ticker.C {

	// }

	termui.Loop()

	for _, w := range sch.workers {
		totalItemNum += w.fetechedItemNum
		totalSavedItemNum += w.savedItemNum
	}

	fmt.Printf("total: %d , total save: %d \n", totalItemNum, totalSavedItemNum)
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

func strToTimeNoT(timeStr string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", timeStr)
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
