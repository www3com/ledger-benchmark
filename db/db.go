package db

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"os"
)

var DB *gorm.DB

func Setup() {
	dsn := fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s sslmode=disable", "localhost", 5433, "postgres", "vdx_oms", "postgres")

	var err error
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		//Logger: logger2.Default.LogMode(logger2.Info),
	})
	if err != nil {
		fmt.Errorf("初始化数据库连接错误: %v", err.Error())
		os.Exit(1)
	}

	//fmt.Println("初始化数据库连接成功")
}
