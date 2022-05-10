package model

import (
	"benchmark/db"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

type Ledger struct {
	ID              int64 `gorm:"type:bigint(20);auto_increment;"`
	DebitAccountId  int64
	CreditAccountId int64
	CurrencyId      int
	Amount          decimal.Decimal
	Action          int
	ReferenceId     int64
	CreateTime      int64
}

func (Ledger) TableName() string {
	return "oms_ledger"
}

func InsertLedgers(tx *gorm.DB, ledgers []*Ledger) error {
	if err := tx.CreateInBatches(ledgers, len(ledgers)).Error; err != nil {
		return err
	}
	return nil
}

func TruncateLedger() error {
	sql := `truncate oms_ledger`
	if err := db.DB.Exec(sql).Error; err != nil {
		return err
	}
	return nil
}
