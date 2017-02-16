package main

import "time"
import "fmt"
import "github.com/astaxie/beego/orm"
import _ "github.com/go-sql-driver/mysql"
import "github.com/golang/glog"

var (
	MysqlORM orm.Ormer
)

type conversionRaw struct {
	ID           int       `orm:"column(id);pk"`
	RawData      string    `orm:"column(raw_data)"`
	ConversionID string    `orm:"column(conversion_id)"`
	CreatedAt    time.Time `orm:"column(created_at);type(timestamp)"`
	UpdatedAt    time.Time `orm:"column(updated_at);type(timestamp)"`
}

func (c *conversionRaw) TableName() string {
	return "affi_conversion_raw"
}

func (c *conversionRaw) insert() error {
	_, err := MysqlORM.Insert(c)
	return err
}

type conversion struct {
	ID                  int       `orm:"column(id);pk;auto"`
	PayTime             int       `orm:"column(pay_time)"`
	PayTimeDay          int       `orm:"column(pay_time_day)"`
	ConversionID        string    `orm:"column(conversion_id)"`
	UID                 int       `orm:"column(uid)"`
	AppID               string    `orm:"column(app_id)"`
	CustomerRef         string    `orm:"column(customer_reference)"`
	ConversionStatus    string    `orm:"column(conversion_status)"`
	Atoken              string    `orm:"column(at)"`
	ConversionValue     float32   `orm:"column(conversion_value)"`
	PublisherCommission float32   `orm:"column(publisher_commission)"`
	PayUserAmount       float32   `orm:"column(pay_user_amount)"`
	ConversionTime      time.Time `orm:"column(conversion_time);type(timestamp)"`
	CreatedAt           time.Time `orm:"column(created_at);type(timestamp)"`
	UpdatedAt           time.Time `orm:"column(updated_at);type(timestamp)"`
	PayedUser           byte      `orm:"column(payed_user)"`
	Type                byte      `orm:"column(type)"`
	InApp               byte      `orm:"column(in_app)"`
}

func (c *conversion) TableName() string {
	return fmt.Sprintf("affi_conversion_%s", c.ConversionTime.Format("200601"))
}

func getConvTableNameByTime(date time.Time) string {
	return fmt.Sprintf("affi_conversion_%s", date.Format("200601"))
}

func (c *conversion) insert() error {
	sql := `INSERT INTO %s 
    (conversion_id, conversion_time, uid, app_id, customer_reference,
    conversion_status, conversion_value, publisher_commission, 
    payed_user, pay_user_amount, pay_time, pay_time_day, type, at, in_app, 
    created_at, updated_at) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)    
    `
	sql = fmt.Sprintf(sql, c.TableName())

	_, err := MysqlORM.Raw(sql, c.ConversionID, c.ConversionTime.Format("2006-01-02T15:04:05"),
		c.UID, c.AppID, c.CustomerRef, c.ConversionStatus, c.ConversionValue,
		c.PublisherCommission, c.PayedUser, c.PayUserAmount, c.PayTime,
		c.PayTimeDay, c.Type, c.Atoken, c.InApp, c.CreatedAt.Format("2006-01-02T15:04:05"),
		c.UpdatedAt.Format("2006-01-02T15:04:05")).Exec()
	return err
}

func (c *conversion) update(status string, conversionVal float32) error {
	tableName := c.TableName()
	sql := fmt.Sprintf(`update %s set conversion_status=?, conversion_value=? where id = ? `, tableName)
	_, err := MysqlORM.Raw(sql, status, conversionVal, c.ID).Exec()
	if err != nil {
		glog.Error(err)
		return err
	}

	return nil
}

func findByConversionID(date time.Time, conversionID string) (*conversion, error) {
	c := conversion{}
	c.ConversionTime = date
	tableName := c.TableName()

	sql := fmt.Sprintf(`select id, conversion_time from %s where conversion_id = ?`, tableName)
	err := MysqlORM.Raw(sql, conversionID).QueryRow(&c)
	if err != nil {
		if err != orm.ErrNoRows {
			glog.Error(err)
		}
		return nil, err
	}

	return &c, nil
}

func InitDB(host, user, pwd, db string) orm.Ormer {
	if MysqlORM == nil {
		info := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", user, pwd, host, db)
		orm.RegisterDriver("mysql", orm.DRMySQL)
		orm.RegisterDataBase("default", "mysql", info)
		// register model
		orm.RegisterModel(new(conversion))
		orm.RegisterModel(new(conversionRaw))
		orm.RegisterModel(new(applePayment))

		MysqlORM = orm.NewOrm()
	}
	return MysqlORM
}
