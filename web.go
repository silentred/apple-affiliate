package main

import (
	"log"

	"golang.org/x/net/websocket"

	"time"

	"encoding/json"

	"strconv"

	"github.com/golang/glog"
	"github.com/labstack/echo"
)

var (
	engine *echo.Echo

	ticker = time.NewTicker(time.Second)
	info   fetchInfo

	ipt *importer
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
	initImporter()

	engine = echo.New()
	engine.POST("/job/fetch", startFetching)
	engine.GET("/status", showStatus)
	engine.POST("/job/import", importApplePaymentData)
	engine.GET("/import/warning", getImporterErrors)

	log.Fatal(engine.Start(":7100"))
}

func initImporter() {
	ipt = newImporter("/tmp")
	go ipt.Start()
}

// POST recieve job
func startFetching(cxt echo.Context) error {
	var job webJob
	err := cxt.Bind(&job)
	if err != nil {
		glog.Error(err)
		return err
	}

	cxt.Response().Header().Add("Access-Control-Allow-Origin", "*")

	// reset info
	info = fetchInfo{}

	if info.isWorking() {
		cxt.JSON(403, echo.Map{"error_code": 1, "message": "scheduler is working"})
		return nil
	}

	fromTime, err := strToTimeNoT(job.FromDate)
	toTime, err := strToTimeNoT(job.ToDate)

	if err != nil {
		return cxt.JSON(403, echo.Map{"error_code": 2, "message": err})
	}

	// delete error warnings from importer
	for key := range ipt.errs.list {
		if key.Unix() <= toTime.Unix() && key.Unix() >= fromTime.Unix() {
			ipt.errs.deleteError(key)
		}
	}

	jobs, err := seperateJobs(fromTime, toTime, jobNum)
	if err != nil {
		return cxt.JSON(403, echo.Map{"error_code": 2, "message": err})
	}

	Scheduler.receiveJobs(jobs)
	cxt.JSON(200, echo.Map{"error_code": 0})

	return nil
}

// websocket
func showStatus(c echo.Context) error {
	websocket.Handler(func(ws *websocket.Conn) {
		defer ws.Close()
		// Write
		for range ticker.C {
			Scheduler.totalProcess(&info)
			err := websocket.Message.Send(ws, info.String())
			if err != nil {
				log.Printf("%#v \n", err)
				break
			}
		}
	}).ServeHTTP(c.Response(), c.Request())
	return nil
}

func importApplePaymentData(c echo.Context) error {
	idStr := c.FormValue("id")
	id, err := strconv.Atoi(idStr)

	c.Response().Header().Add("Access-Control-Allow-Origin", "*")

	if err != nil {
		c.JSON(403, echo.Map{"error_code": 2, "message": err})
		return err
	}

	ipt.addJob(id)

	c.JSON(200, echo.Map{"error_code": 0})
	return nil
}

type errorInfo struct {
	Date   string `json:"date"`
	ConvID string `json:"conv_id"`
}

func getImporterErrors(c echo.Context) error {
	c.Response().Header().Add("Access-Control-Allow-Origin", "*")
	list := []errorInfo{}
	for key, val := range ipt.errs.list {
		list = append(list, errorInfo{Date: key.Format(time.RFC3339), ConvID: val})
	}
	c.JSON(200, echo.Map{"error_code": 0, "data": list})
	return nil
}
