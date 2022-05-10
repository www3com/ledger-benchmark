package model

import (
	"benchmark/db"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

type Asset struct {
	ID         int64 `gorm:"primary_key"`
	AccountId  int64
	CurrencyId int
	Amount     decimal.Decimal `gorm:"column:available"`
	Frozen     decimal.Decimal
	Fee        decimal.Decimal `gorm:"-"`
	Direct     int8            `gorm:"-"` // 1:买，2：卖
	CreateTime int64
	UpdateTime int64
}

func (Asset) TableName() string {
	return "oms_asset"
}

func UpdateAsset(tx *gorm.DB, asset *Asset) error {
	sign := "+"
	if asset.Direct == 2 {
		sign = "-"
	}
	sql := `UPDATE oms_asset
        SET available   = available ` + sign + ` ?,
            update_time = ?
        WHERE account_id = ?
          AND currency_id = ?`
	if err := tx.Exec(sql, asset.Amount, asset.UpdateTime, asset.AccountId, asset.CurrencyId).Error; err != nil {
		return err
	}
	return nil
}

func TruncateAsset() error {
	sql := `truncate oms_asset`
	if err := db.DB.Exec(sql).Error; err != nil {
		return err
	}
	return nil
}

func InsertAsset(assets []*Asset) error {
	if err := db.DB.CreateInBatches(assets, len(assets)).Error; err != nil {
		return err
	}
	return nil
}
