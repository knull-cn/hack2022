package repo

import (
	"fmt"
	beego "github.com/beego/beego/v2/server/web"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var (
	db *gorm.DB
)

func GetDB() *gorm.DB {
	return db
}
func InitDB() error {
	ip, err := beego.AppConfig.String("mysql.ip")
	if err != nil {
		return err
	}
	port, err := beego.AppConfig.String("mysql.port")
	if err != nil {
		return err
	}
	username, err := beego.AppConfig.String("mysql.username")
	if err != nil {
		return err
	}
	password, err := beego.AppConfig.String("mysql.password")
	if err != nil {
		return err
	}
	dbName, err := beego.AppConfig.String("mysql.database")
	if err != nil {
		return err
	}
	dbUrl := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", username, password, ip, port, dbName)

	db, err = gorm.Open(mysql.Open(dbUrl), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	})
	if err != nil {
		return err
	}
	return db.AutoMigrate()
}
