package ledger

import "time"

type AccountType string
const (
	Asset      AccountType = "ASSET"
	Liability  AccountType = "LIABILITY"
	Revenue    AccountType = "REVENUE"
	Expense    AccountType = "EXPENSE"
	Settlement AccountType = "SETTLEMENT"
)

type EntryType string
const (
	Debit  EntryType = "DEBIT"
	Credit EntryType = "CREDIT"
)

type LedgerAccount struct {
	AccountID   string
	AccountName string
	AccountType AccountType
	Currency    string
	Status      string
}

type LedgerEntry struct {
	AccountID string
	EntryType EntryType
	Amount    int64
}

type Journal struct {
	JournalID       string
	TrackingID      string
	TransactionType string
	Entries         []LedgerEntry
	CreatedAt       time.Time
}