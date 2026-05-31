package domecore

import "google.golang.org/genproto/googleapis/type/money"

func MinorToMoney(amount int64, currency string) *money.Money {
	units := amount / 100
	nanos := (amount % 100) * 10000000

	return &money.Money{
		CurrencyCode: currency,
		Units:        units,
		Nanos:        int32(nanos),
	}
}

func MoneyToMinor(m *money.Money) int64 {
	return m.Units*100 + int64(m.Nanos)/10000000
}
