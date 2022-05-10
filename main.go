package main

import (
	"benchmark/db"
	"benchmark/model"
	"bufio"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"os"
	"strings"
	"time"
)

var (
	aAccountId int64 = 1
	bAccountId int64 = 2
	tradeHouse int64 = 100
	feeHouse   int64 = 200
	// 交易对：BTC/USD
	symbolId = 210001

	feeRate = 0.001

	// BTC
	btcId = 110001
	// USD
	usdId = 110002
	// 执行报告数量: 100万
	row int64 = 1000000

	// 批量接收6条执行报告
	batchSize = 5
	batchEr   = make([]*ExecutionReport, 0, batchSize)
)

type ExecutionReport struct {
	symbolId       int
	execId         int64
	price          decimal.Decimal
	quantity       decimal.Decimal
	amount         decimal.Decimal
	takerAccountId int64
	takerSide      int // 1: 买， 2：卖
	makerAccountId int64
}

func init() {
	db.Setup()
}

func main() {

	// 清理现场，初始化账户数据
	fmt.Print("需要删除账户、账本数据，请确认是否继续(Y/N)？")
	input := bufio.NewScanner(os.Stdin)
	input.Scan()
	line := input.Text()
	if strings.ToUpper(line) != "Y" {
		fmt.Print("退出程序")
		return
	}

	clearAndInitData()

	start := time.Now().Unix()
	var i int64
	for i = 1; i <= row; i++ {
		er := ExecutionReport{
			symbolId:       symbolId,
			execId:         i,
			price:          decimal.NewFromFloat32(1),
			quantity:       decimal.NewFromFloat32(1),
			amount:         decimal.NewFromFloat32(1),
			takerAccountId: aAccountId,
			takerSide:      1,
			makerAccountId: bAccountId,
		}
		handle(&er, time.Now().UnixMicro())
	}

	end := time.Now().Unix()

	println("总耗时： ", end-start)
}

func handle(er *ExecutionReport, now int64) {
	batchEr = append(batchEr, er)
	if len(batchEr) < batchSize && er.execId < row {
		return
	}

	var assets []*model.Asset
	var ledgers []*model.Ledger

	for _, report := range batchEr {
		// 账户A btc 增加， usd减少； 账户B btc 减少， usd增加
		tBtc := getTakerAsset(report, true)
		tUsd := getTakerAsset(report, false)
		mBtc := getMakerAsset(report, true)
		mUsd := getMakerAsset(report, false)

		assets = append(assets, tBtc)
		assets = append(assets, tUsd)
		assets = append(assets, mBtc)
		assets = append(assets, mUsd)

		ledgers = generateLedger(ledgers, tBtc, report.execId, now)
		ledgers = generateLedger(ledgers, tUsd, report.execId, now)
		ledgers = generateLedger(ledgers, mBtc, report.execId, now)
		ledgers = generateLedger(ledgers, mUsd, report.execId, now)

	}

	err := db.DB.Transaction(func(tx *gorm.DB) error {
		for _, asset := range assets {
			if err := model.UpdateAsset(tx, asset); err != nil {
				return errors.New(fmt.Sprintf("更新资产错误： %v", err))
			}
		}
		if err := model.InsertLedgers(tx, ledgers); err != nil {
			return errors.New(fmt.Sprintf("插入账本错误： %v", err))
		}
		return nil
	})

	if err != nil {
		fmt.Errorf("错误退出, %v", err)
		os.Exit(1)
	}

	batchEr = make([]*ExecutionReport, 0, batchSize)
}

func generateLedger(ledgers []*model.Ledger, asset *model.Asset, referenceId int64, now int64) []*model.Ledger {
	var debitAccountId int64
	var creditAccountId int64
	if asset.Direct == 1 {
		debitAccountId = tradeHouse
		creditAccountId = asset.AccountId
	} else {
		debitAccountId = asset.AccountId
		creditAccountId = tradeHouse
	}

	ledger := model.Ledger{
		DebitAccountId:  debitAccountId,
		CreditAccountId: creditAccountId,
		CurrencyId:      asset.CurrencyId,
		Amount:          asset.Amount,
		Action:          1,
		ReferenceId:     referenceId,
		CreateTime:      now,
	}
	ledgers = append(ledgers, &ledger)

	if asset.Direct == 1 {
		feeLedger := model.Ledger{
			DebitAccountId:  asset.AccountId,
			CreditAccountId: feeHouse,
			CurrencyId:      asset.CurrencyId,
			Amount:          asset.Fee,
			Action:          1,
			ReferenceId:     referenceId,
			CreateTime:      now,
		}
		ledgers = append(ledgers, &feeLedger)
	}

	return ledgers
}

func getTakerAsset(report *ExecutionReport, isBaseCurrency bool) *model.Asset {
	asset := model.Asset{}
	asset.AccountId = report.takerAccountId
	if report.takerSide == 1 {
		setAmount(report, true, isBaseCurrency, &asset)
	} else {
		setAmount(report, false, isBaseCurrency, &asset)
	}
	return &asset
}

func getMakerAsset(report *ExecutionReport, isBaseCurrency bool) *model.Asset {
	asset := model.Asset{}
	asset.AccountId = report.makerAccountId
	if report.takerSide == 2 {
		setAmount(report, true, isBaseCurrency, &asset)
	} else {
		setAmount(report, false, isBaseCurrency, &asset)
	}
	return &asset
}

func setAmount(report *ExecutionReport, isBuy bool, isBaseCurrency bool, asset *model.Asset) {

	if isBaseCurrency {
		asset.CurrencyId = btcId
		asset.Amount = report.amount
		if isBuy {
			asset.Direct = 1
			fee := asset.Amount.Mul(decimal.NewFromFloat32(float32(feeRate)))
			asset.Amount = asset.Amount.Sub(fee)
			asset.Fee = fee
		} else {
			asset.Direct = 2
		}
	} else {
		asset.CurrencyId = usdId
		asset.Amount = report.quantity
		if isBuy {
			asset.Direct = 2
		} else {
			asset.Direct = 1
			fee := asset.Amount.Mul(decimal.NewFromFloat32(float32(feeRate)))
			asset.Amount = asset.Amount.Sub(fee)
			asset.Fee = fee
		}
	}
}

func clearAndInitData() {
	err := model.TruncateAsset()
	if err != nil {
		fmt.Print("清理资产数据错误")
		return
	}
	err = model.TruncateLedger()
	if err != nil {
		fmt.Print("清理账本数据错误")
		return
	}

	now := time.Now().UnixMicro()
	assets := []*model.Asset{
		{
			AccountId:  aAccountId,
			CurrencyId: btcId,
			Amount:     decimal.NewFromFloat32(0),
			CreateTime: now,
			UpdateTime: now,
		}, {
			AccountId:  aAccountId,
			CurrencyId: usdId,
			Amount:     decimal.NewFromFloat32(10000000000000),
			CreateTime: now,
			UpdateTime: now,
		}, {
			AccountId:  bAccountId,
			CurrencyId: btcId,
			Amount:     decimal.NewFromFloat32(10000000000000),
			CreateTime: now,
			UpdateTime: now,
		}, {
			AccountId:  bAccountId,
			CurrencyId: usdId,
			Amount:     decimal.NewFromFloat32(0),
			CreateTime: now,
			UpdateTime: now,
		},
	}
	_ = model.InsertAsset(assets)
}
