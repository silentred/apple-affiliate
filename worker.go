package main

import (
	"strings"
	"time"

	"fmt"

	"encoding/json"

	"flag"

	"strconv"

	"github.com/golang/glog"
)

var (
	appKey = ""
	apiKey = ""

	statusNames map[int]string
)

const (
	statusRunning = iota
	statusStop
	statusError

	atoken      = "1001lpy5"
	publisherID = "1010l19090"
	apiUrl      = "https://%s@itunes-api.performancehorizon.com/reporting/report_publisher/publisher/%s/conversion"
)

type fetchWorker struct {
	id     int
	status int
	// fetched item number
	fetechedItemNum int
	savedItemNum    int
	// job
	currJob      job
	lastConvTime time.Time
	// stop signal
	stop chan struct{}
	sch  *scheduler
}

func init() {
	flag.StringVar(&appKey, "appKey", "", "appKey of apple")
	flag.StringVar(&apiKey, "apiKey", "", "apiKey of apple")

	statusNames = map[int]string{
		statusStop:    "stop",
		statusRunning: "running",
		statusError:   "error",
	}
}

func newFetchWorker(j job, sch *scheduler) *fetchWorker {
	w := &fetchWorker{
		id:           sch.workerID,
		status:       statusStop,
		currJob:      j,
		stop:         make(chan struct{}, 1),
		sch:          sch,
		lastConvTime: j.from,
	}

	sch.workerID++

	return w
}

func (w *fetchWorker) statusName() string {
	return statusNames[w.status]
}

func (w *fetchWorker) getStatus() []string {
	return []string{
		strconv.Itoa(w.id),
		w.statusName(),
		strconv.Itoa(w.currJob.offset),
		strconv.Itoa(w.fetechedItemNum),
		strconv.Itoa(w.savedItemNum),
		w.currJob.String(),
	}
}

func (w *fetchWorker) Run() {
	w.status = statusRunning
	for {
		select {
		case <-w.stop:
			return
		default:
			// do default
			_, hasNext := w.doJob()
			if hasNext {
				w.currJob.offset += w.currJob.limit
			} else {
				w.stopWorker()
			}
		}
	}
}

func (w *fetchWorker) stopWorker() {
	w.stop <- struct{}{}
	// change status
	w.status = statusStop
	err := w.sch.rescheduleJob()
	if err != nil {
		glog.Error(err)
	}
}

func (w *fetchWorker) doJob() (error, bool) {
	var err error
	var body []byte
	var retry = 3
	var needRetry = true

	for retry > 0 && needRetry {
		body, err = w.fetchAppleAPI()
		if err != nil {
			glog.Error(err)
			retry--
		} else {
			needRetry = false
		}
	}

	if err != nil {
		// TODO log error job
		glog.Error(w.currJob.String())
		return err, true
	}

	err, hasNext := w.resolveConversions(body)
	if err != nil {
		glog.Error(err)
		return err, hasNext
	}

	return nil, hasNext
}

func (w *fetchWorker) fetchAppleAPI() ([]byte, error) {
	var url, basicAuth string
	params := map[string]string{
		"convert_currency": "USD",
		"offset":           strconv.Itoa(w.currJob.offset),
		"limit":            strconv.Itoa(w.currJob.limit),
		"start_date":       w.currJob.from.Format("2006-01-02 15:04:05"),
		"end_date":         w.currJob.to.Format("2006-01-02 15:04:05"),
	}

	c := NewReqeustConfig(params, nil, 90, nil, nil)

	basicAuth = fmt.Sprintf("%s:%s", appKey, apiKey)
	url = fmt.Sprintf(apiUrl, basicAuth, publisherID)

	body, _, err := HTTPGet(url, c)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func (w *fetchWorker) resolveConversions(body []byte) (error, bool) {
	var list conversionList
	var hasNext bool

	err := json.Unmarshal(body, &list)
	if err != nil {
		return err, false
	}

	if list.Hypermedia.Pagination.NextPage != "" {
		hasNext = true
	}

	if len(list.Conversions) == 0 {
		// TODO log api error
		glog.Error(w.currJob.String())
	}

	for _, item := range list.Conversions {
		c, err := item.ConvData.toConversion()
		if err != nil {
			glog.Error(err)
			continue
		}
		w.lastConvTime = c.ConversionTime

		w.fetechedItemNum++
		// insert to db
		if c.ConversionValue > 0 {
			conv, _ := findByConversionID(c.ConversionTime, c.ConversionID)
			if conv == nil {
				err = c.insert()
				if err != nil {
					glog.Error(err)
					continue
				}
			} else {
				if item.ConvData.Value.Status != c.ConversionStatus {
					err = conv.update(item.ConvData.Value.Status, item.ConvData.Value.Value)
					if err != nil {
						glog.Error(err)
						continue
					}
				}
			}
		}
		w.savedItemNum++
	}

	return nil, hasNext
}

type conversionList struct {
	Conversions []struct {
		ConvData conversionData `json:"conversion_data"`
	} `json:"conversions"`

	Hypermedia struct {
		Pagination struct {
			NextPage string `json:"next_page"`
		} `json:"pagination"`
	} `json:"hypermedia"`
}

type conversionData struct {
	ID             string    `json:"conversion_id"`
	ConversionTime string    `json:"conversion_time"`
	PublisherRef   string    `json:"publisher_reference"`
	AdvRef         string    `json:"advertiser_reference"`
	CustomerRef    string    `json:"customer_reference"`
	Value          convValue `json:"conversion_value"`
}

type convValue struct {
	Status              string  `json:"conversion_status"`
	Value               float32 `json:"value"`
	PublisherCommission float32 `json:"publisher_commission"`
}

func (c *conversionData) toConversion() (*conversion, error) {
	var uidStr, appID string
	var typeVal, inApp byte
	var uid int

	t, err := strToTimeForConv(c.ConversionTime)
	if err != nil {
		return nil, err
	}

	info := strings.Split(c.PublisherRef, ":")
	if len(info) >= 2 {
		uidStr = info[0]
		appID = info[1]
	}
	if strings.HasPrefix(uidStr, "u") {
		uidStr = uidStr[1:]
		typeVal = 0
	} else {
		typeVal = 1
	}

	uid, err = strconv.Atoi(uidStr)
	if err != nil {
		glog.Errorf("convert uidStr %s to int failed", uidStr)
		return nil, err
	}

	if strings.Contains(c.AdvRef, "In-App") {
		inApp = 1
	}

	conv := conversion{
		ConversionID:        c.ID,
		ConversionTime:      t,
		UID:                 uid,
		AppID:               appID,
		CustomerRef:         c.CustomerRef,
		ConversionStatus:    c.Value.Status,
		ConversionValue:     c.Value.Value,
		PublisherCommission: c.Value.PublisherCommission,
		PayedUser:           0,
		PayUserAmount:       c.Value.PublisherCommission * 0.05,
		PayTime:             int(t.Unix()),
		PayTimeDay:          t.Day(),
		Atoken:              atoken,
		Type:                typeVal,
		InApp:               inApp,
		CreatedAt:           time.Now(),
		UpdatedAt:           time.Now(),
	}

	return &conv, nil
}

func strToTimeForConv(timeStr string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", timeStr)
}
