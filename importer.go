package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"sync"
	"time"

	"net/url"

	"os"
	"path/filepath"

	"strconv"

	"github.com/golang/glog"
)

const (
	IndexConvID        = 0
	IndexConvTime      = 4
	IndexCurrency      = 8
	IndexPublisherComm = 17
	IndexConvValue     = 26

	maxJobQueue = 10

	notImported = 0
	importing   = 1
	imported    = 2
)

type importer struct {
	jobNum int      // current job number
	jobs   chan int // chan of applePayment ID
	mutex  sync.Mutex
	csvDir string
	errs   errorList
}

type errorList struct {
	list map[time.Time]string
	mut  sync.Mutex
}

type applePayment struct {
	ID           int     `orm:"column(id);pk"`
	Reference    string  `orm:"column(reference)"`
	CsvFile      string  `orm:"column(csv_file)"`
	Imported     int     `orm:"column(imported)"` // 0 not start, 1 running, 2 done
	ExchangeRate float64 `orm:"column(exchange_rate)"`
	TotalValue   float64 `orm:"column(total_value)"` // 本地货币
	PaidAmount   float64 `orm:"column(paid_amount)"` // 实际支付美金
}

func (a *applePayment) TableName() string {
	return "affi_sdk_apple_payment"
}

func (a *applePayment) updateStatus() error {
	_, err := MysqlORM.Update(a, "Imported")
	return err
}

func findApplePaymentByID(id int) (applePayment, error) {
	p := applePayment{ID: id}
	err := MysqlORM.Read(&p)
	if err != nil {
		return p, err
	}
	return p, nil
}

// csv 中的单行数据结构
type appleConv struct {
	ConvID          string
	ConvTime        time.Time
	AppleAmount     float64
	AppleCurrency   string
	AppleAmountUSD  float64
	OriginConvValue float64
}

// 更新 conversion 状态
func udpateConvByApple(conv appleConv, reference string) error {
	var num int64
	var err error

	tableName := getConvTableNameByTime(conv.ConvTime)
	sql := `update %s set apple_payed_us=1, apple_amount=?, apple_currency=?, 
	apple_amount_usd=?, conversion_value_origin=? where conversion_id=?`
	sql = fmt.Sprintf(sql, tableName)

	result, err := MysqlORM.Raw(sql, conv.AppleAmount, conv.AppleCurrency,
		conv.AppleAmountUSD, conv.OriginConvValue, conv.ConvID).Exec()

	if err == nil && result != nil {
		num, err = result.RowsAffected()
	} else {
		glog.Errorf("%#v, time=%s convID=%s", err, conv.ConvTime.Format(time.RFC3339), conv.ConvID)
		return err
	}

	if num == 0 {
		_, err = findByConversionID(conv.ConvTime, conv.ConvID)
		if err != nil {
			glog.Errorf("NotFound time=%s convID=%s file=%s", conv.ConvTime.Format(time.RFC3339), conv.ConvID, reference)
			ipt.errs.setError(conv.ConvTime, fmt.Sprintf("ID=%s, Ref=%s", conv.ConvID, reference))
		}
	}

	return nil
}

func newImporter(dir string) *importer {
	if !FileExists(dir) {
		panic(dir + " dir not exists")
	}

	i := &importer{
		jobs:   make(chan int, maxJobQueue),
		mutex:  sync.Mutex{},
		csvDir: dir,
		errs: errorList{
			list: make(map[time.Time]string),
			mut:  sync.Mutex{},
		},
	}
	return i
}

func (i *importer) addJob(id int) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	if i.jobNum >= maxJobQueue {
		return fmt.Errorf("too many jobs")
	}
	i.jobNum++
	i.jobs <- id

	return nil
}

func (i *importer) Start() {
	for {
		id := <-i.jobs
		// decreate jobNum
		i.mutex.Lock()
		i.jobNum--
		i.mutex.Unlock()

		applePay, err := findApplePaymentByID(id)
		if err != nil {
			glog.Error(err)
			return
		}

		if applePay.Imported != 0 || applePay.PaidAmount == 0 || applePay.ExchangeRate == 0 {
			glog.Error("applePay.PaidAmount is 0 or it has been imported, %v", applePay)
			continue
		}

		fmt.Println("update status 1...")
		applePay.Imported = importing
		err = applePay.updateStatus()
		if err != nil {
			glog.Error(err)
			return
		}

		fmt.Println("preparing...")
		err = i.prepareJob(applePay)
		if err != nil {
			glog.Error(err)
			return
		}

		fmt.Println("handling csv...")
		err = i.handleCsv(applePay)
		if err != nil {
			glog.Error(err)
			return
		}

		fmt.Println("update status 2...")
		applePay.Imported = imported
		err = applePay.updateStatus()
		if err != nil {
			glog.Error(err)
			return
		}
	}
}

func (i *importer) prepareJob(applePay applePayment) error {
	// download file
	url := addAuthToURL(applePay.CsvFile, appKey, apiKey)
	config := NewReqeustConfig(nil, nil, 600, nil, nil)
	tmpFile, _, _, err := HTTPGetFile(url, config)
	if err != nil {
		return err
	}

	destFile := filepath.Join(i.csvDir, applePay.Reference+".zip")
	err = os.Rename(tmpFile, destFile)
	if err != nil {
		return err
	}

	err = Unzip(destFile, i.csvDir)
	if err != nil {
		return err
	}

	return nil
}

func (i *importer) handleCsv(applePay applePayment) error {
	csvFile := filepath.Join(i.csvDir, applePay.Reference+".csv")
	if !FileExists(csvFile) {
		return fmt.Errorf("file %s not exists", csvFile)
	}

	file, err := os.Open(csvFile)
	if err != nil {
		return err
	}

	defer file.Close()
	reader := csv.NewReader(file)

	lineCount := 0
	for {
		record, err := reader.Read()
		lineCount++
		// ignore first line
		if lineCount == 1 {
			continue
		}

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		timeStr := record[IndexConvTime]
		time, err := strToTimeNoT(timeStr)
		if err != nil {
			glog.Error(err)
			continue
		}

		amount, err := strconv.ParseFloat(record[IndexPublisherComm], 64)
		if err != nil {
			glog.Error(err)
			continue
		}
		originConvValue, err := strconv.ParseFloat(record[IndexConvValue], 64)
		if err != nil {
			glog.Error(err)
			continue
		}

		amountUSD := amount
		if record[IndexCurrency] != "USD" {
			amountUSD = amount / applePay.ExchangeRate
			if err != nil {
				glog.Error(err)
				continue
			}
		}

		conv := appleConv{
			ConvID:          record[IndexConvID],
			ConvTime:        time,
			AppleCurrency:   record[IndexCurrency],
			AppleAmount:     amount,
			AppleAmountUSD:  amountUSD,
			OriginConvValue: originConvValue,
		}

		err = udpateConvByApple(conv, applePay.Reference)
		if err != nil {
			glog.Errorf("time=%s , convID=%s", timeStr, conv.ConvID)
		}

	}

	return nil
}

func addAuthToURL(URL, user, pwd string) string {
	u, err := url.Parse(URL)
	if err != nil {
		glog.Error(err)
		return URL
	}

	u.User = url.UserPassword(user, pwd)
	return u.String()
}

func (e *errorList) setError(date time.Time, convID string) {
	e.mut.Lock()
	e.list[date] = convID
	e.mut.Unlock()
}

func (e *errorList) deleteError(date time.Time) {
	e.mut.Lock()
	delete(e.list, date)
	e.mut.Unlock()
}
