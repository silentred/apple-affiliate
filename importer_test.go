package main

import (
	"fmt"
	"testing"
)

import "github.com/stretchr/testify/assert"

func TestURL(t *testing.T) {
	url := "https://itunes-api.performancehorizon.com/user/publisher/1010l19090/selfbill/S-1010l19090-1000l24697/items.csv"
	url = addAuthToURL(url, "user", "pwd")
	fmt.Println(url)
}

func TestApplePayment(t *testing.T) {
	InitDB("127.0.0.1:3306", "jason", "jason", "fenda")
	appKey = "yYit5mQdd1"
	apiKey = "SHba2kUI"

	i := newImporter("/tmp")
	id := 3
	apple, _ := findApplePaymentByID(id)
	err := i.prepareJob(apple)
	assert.NoError(t, err)
	err = i.handleCsv(apple)
	assert.NoError(t, err)
}

func TestPrecise(t *testing.T) {
	InitDB("127.0.0.1:3306", "jason", "jason", "fenda")
	appKey = "yYit5mQdd1"
	apiKey = "SHba2kUI"

	tm, _ := strToTime("2016-11-01T00:00:01")
	conv := appleConv{
		ConvID:          "1000l893029726xxx",
		ConvTime:        tm,
		AppleAmount:     0.38,
		AppleCurrency:   "USD",
		AppleAmountUSD:  0.38,
		OriginConvValue: 3.99,
	}

	err := udpateConvByApple(conv)
	assert.NoError(t, err)
}
