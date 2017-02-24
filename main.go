package main

import (
	"flag"
	"log"
)

var (
	fromDateStr string
	toDateStr   string
	jobNum      int

	mysqlHost string
	mysqlUser string
	mysqlPwd  string
	mysqlDB   string

	isWeb bool

	Scheduler *scheduler
)

func init() {
	flag.StringVar(&fromDateStr, "from", "", "for example: 2017-02-01 00:00:00")
	flag.StringVar(&toDateStr, "to", "", "for example: 2017-02-02 00:00:00")
	flag.IntVar(&jobNum, "go", 1, "goroutine number")

	flag.StringVar(&mysqlHost, "host", "127.0.0.1", "mysql host")
	flag.StringVar(&mysqlUser, "user", "root", "mysql user")
	flag.StringVar(&mysqlPwd, "pwd", "", "mysql password")
	flag.StringVar(&mysqlDB, "db", "fenda", "mysql db")

	flag.BoolVar(&isWeb, "web", false, "use web interface")
}

func main() {
	flag.Parse()
	InitDB(mysqlHost, mysqlUser, mysqlPwd, mysqlDB)
	Scheduler = newScheduler(jobNum)
	Scheduler.createWorker(jobNum)

	if !isWeb {
		startCmd()
	} else {
		startWeb()
	}
}

func startCmd() {
	fromTime, err := strToTime(fromDateStr)
	toTime, err := strToTime(toDateStr)
	if err != nil {
		log.Fatalln(err)
	}

	jobs, err := seperateJobs(fromTime, toTime, jobNum)
	if err != nil {
		log.Fatalln(err)
	}

	Scheduler.receiveJobs(jobs)
	//Scheduler.printProcess()
	Scheduler.printProcessWithUI()
}
