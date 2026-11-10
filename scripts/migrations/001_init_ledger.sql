-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- -----------------------------------------------------------------------------
-- 1. Accounts Table (Stores master balances and account status)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_number VARCHAR(32) NOT NULL UNIQUE,
    holder_name VARCHAR(128) NOT NULL,
    balance NUMERIC(18, 4) NOT NULL DEFAULT 0.0000,
    currency VARCHAR(3) NOT NULL DEFAULT 'IDR',
    status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_positive_balance CHECK (balance >= 0)
);

CREATE INDEX IF NOT EXISTS idx_accounts_account_number ON accounts(account_number);
CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);

-- -----------------------------------------------------------------------------
-- 2. Transactions Table (Header of each financial movement)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reference_id VARCHAR(64) NOT NULL UNIQUE,
    description TEXT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'POSTED' CHECK (status IN ('PENDING', 'POSTED', 'FAILED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_reference_id ON transactions(reference_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);

-- -----------------------------------------------------------------------------
-- 3. Entries Table (Double-Entry Ledger Items)
-- Invariant: For each transaction, SUM(DEBIT) must equal SUM(CREDIT)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE RESTRICT,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    direction VARCHAR(6) NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount NUMERIC(18, 4) NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entries_transaction_id ON entries(transaction_id);
CREATE INDEX IF NOT EXISTS idx_entries_account_id ON entries(account_id);

-- -----------------------------------------------------------------------------
-- 4. Outbox Events Table (For Transactional Outbox Pattern to Kafka)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(32) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_status_created ON outbox_events(status, created_at)
WHERE status = 'PENDING';

-- -----------------------------------------------------------------------------
-- Seed Initial Accounts for Testing / Simulation
-- -----------------------------------------------------------------------------
INSERT INTO accounts (id, account_number, holder_name, balance, currency, status)
VALUES 
    ('a0000000-0000-0000-0000-000000000001', 'ACC-ALICE-01', 'Alice Wijaya', 10000000.0000, 'IDR', 'ACTIVE'),
    ('b0000000-0000-0000-0000-000000000002', 'ACC-BOB-02', 'Bob Pratama', 2500000.0000, 'IDR', 'ACTIVE'),
    ('c0000000-0000-0000-0000-000000000003', 'ACC-CHARLIE-03', 'Charlie Merchant', 50000000.0000, 'IDR', 'ACTIVE')
ON CONFLICT (account_number) DO NOTHING;
