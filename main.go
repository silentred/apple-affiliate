package main

import "flag"

var (
	fromDateStr string
	toDateStr   string
	jobNum      int

	mysqlHost string
	mysqlUser string
	mysqlPwd  string
	mysqlDB   string
)

func init() {
	flag.StringVar(&fromDateStr, "from", "", "for example: 2017-02-01 00:00:00")
	flag.StringVar(&toDateStr, "to", "", "for example: 2017-02-02 00:00:00")
	flag.IntVar(&jobNum, "go", 1, "goroutine number")

	flag.StringVar(&mysqlHost, "host", "127.0.0.1", "mysql host")
	flag.StringVar(&mysqlUser, "user", "root", "mysql user")
	flag.StringVar(&mysqlPwd, "pwd", "", "mysql password")
	flag.StringVar(&mysqlDB, "db", "fenda", "mysql db")
}

func main() {
	flag.Parse()
	InitDB(mysqlHost, mysqlUser, mysqlPwd, mysqlDB)
}
