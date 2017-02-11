package main

import (
	"log"

	"golang.org/x/net/websocket"

	"time"

	"encoding/json"

	"github.com/golang/glog"
	"github.com/labstack/echo"
)

var (
	engine *echo.Echo

	ticker = time.NewTicker(time.Second)
	info   fetchInfo
)

type fetchInfo struct {
	WorkerNum  int `json:"worker_num"`
	Offset     int `json:"offset"`
	FetchedNum int `json:"fetched_num"`
	SavedNum   int `json:"saved_num"`
	StopNum    int `json:"stop_num"`
}

type webJob struct {
	FromDate string `form:"from_date"`
	ToDate   string `form:"to_date"`
}

func (f *fetchInfo) String() string {
	b, _ := json.Marshal(*f)
	return string(b)
}

func (f *fetchInfo) isWorking() bool {
	return f.WorkerNum != 0 && f.WorkerNum != f.StopNum
}

func startWeb() {
	engine = echo.New()
	engine.POST("/job/fetch", startFetching)
	engine.GET("/status", showStatus)

	log.Fatal(engine.Start(":7100"))
}

// POST recieve job
func startFetching(cxt echo.Context) error {
	var job webJob
	err := cxt.Bind(&job)
	if err != nil {
		glog.Error(err)
		return err
	}

	if info.isWorking() {
		cxt.JSON(403, echo.Map{
			"error_code": 1,
			"message":    "scheduler is working",
		})
		return nil
	}

	fromTime, err := strToTime(job.FromDate)
	toTime, err := strToTime(job.ToDate)
	if err != nil {
		cxt.JSON(403, echo.Map{
			"error_code": 2,
			"message":    err,
		})
	}
	jobs, err := seperateJobs(fromTime, toTime, jobNum)
	if err != nil {
		cxt.JSON(403, echo.Map{
			"error_code": 2,
			"message":    err,
		})
	}

	Scheduler.receiveJobs(jobs)
	cxt.JSON(200, echo.Map{"error_code": 0})

	return nil
}

// websocket
func showStatus(c echo.Context) error {
	websocket.Handler(func(ws *websocket.Conn) {
		defer ws.Close()
		for {
			// Write
			for range ticker.C {
				Scheduler.totalProcess(&info)
				err := websocket.Message.Send(ws, info.String())
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}).ServeHTTP(c.Response(), c.Request())
	return nil
}
