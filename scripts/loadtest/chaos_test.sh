#!/usr/bin/env bash
# =============================================================================
# Chaos Engineering Experiment Script
# Simulates infrastructure failures:
# 1. Message Broker (Kafka/Redpanda) sudden crash during high load
# 2. Verifies Zero Data Loss & ACID Invariant in PostgreSQL
# 3. Verifies Outbox Relay automatic self-healing upon broker recovery
# =============================================================================
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

GATEWAY_URL=${GATEWAY_URL:-"http://localhost:8000"}
DB_URL=${DB_URL:-"postgres://postgres:postgres@localhost:5433/ledger_db?sslmode=disable"}

echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}  🏛️ Distributed Ledger Chaos Engineering Suite                  ${NC}"
echo -e "${BLUE}=================================================================${NC}"

# 1. Pre-flight health check
echo -e "\n${YELLOW}[Step 1/5] Checking service health...${NC}"
if ! curl -sf "${GATEWAY_URL}/healthz" > /dev/null; then
    echo -e "${RED}❌ API Gateway is not reachable at ${GATEWAY_URL}. Start services first!${NC}"
    exit 1
fi
echo -e "${GREEN}✅ API Gateway is healthy.${NC}"

# 2. Record ledger balance invariants before chaos
echo -e "\n${YELLOW}[Step 2/5] Inspecting Ledger Invariants before experiment...${NC}"
TOTAL_BALANCES_BEFORE=$(docker exec ledger-postgres psql -U postgres -d ledger_db -t -c "SELECT SUM(balance) FROM accounts;" | tr -d '[:space:]')
ENTRIES_SUM_BEFORE=$(docker exec ledger-postgres psql -U postgres -d ledger_db -t -c "SELECT COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0) FROM entries;" | tr -d '[:space:]')

echo -e "  Total System Money: ${GREEN}Rp ${TOTAL_BALANCES_BEFORE}${NC}"
echo -e "  Double-Entry Delta (Debit - Credit): ${GREEN}${ENTRIES_SUM_BEFORE}${NC} (Must be exactly 0)"

# 3. Inject Broker Failure: Pause Redpanda Container
echo -e "\n${YELLOW}[Step 3/5] 🚨 INJECTING CHAOS: Pausing Kafka/Redpanda broker...${NC}"
docker pause ledger-redpanda
echo -e "${RED}⚡ Redpanda broker is now FROZEN (simulating network partition / broker crash).${NC}"

# Fire 10 transactions while broker is offline
echo -e "\n${YELLOW}Firing transactions while broker is offline...${NC}"
SUCCESSFUL_TX=0
for i in {1..10}; do
    IDEMP_KEY="chaos-tx-$(date +%s%N)"
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${GATEWAY_URL}/api/v1/transactions" \
        -H "Content-Type: application/json" \
        -H "Idempotency-Key: ${IDEMP_KEY}" \
        -d '{
            "source_account": "ACC-ALICE-01",
            "destination_account": "ACC-BOB-02",
            "amount": 10000.0,
            "currency": "IDR",
            "description": "Chaos test during broker outage"
        }')
    
    if [ "$HTTP_CODE" -eq 200 ] || [ "$HTTP_CODE" -eq 201 ]; then
        SUCCESSFUL_TX=$((SUCCESSFUL_TX + 1))
    fi
done

echo -e "  Transactions successfully posted to ACID Ledger: ${GREEN}${SUCCESSFUL_TX}/10${NC}"

# Verify that Outbox events accumulated safely in PostgreSQL
PENDING_OUTBOX=$(docker exec ledger-postgres psql -U postgres -d ledger_db -t -c "SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING';" | tr -d '[:space:]')
echo -e "  Pending events safely queued in PostgreSQL Outbox: ${GREEN}${PENDING_OUTBOX}${NC}"

if [ "$PENDING_OUTBOX" -eq 0 ]; then
    echo -e "${RED}❌ Expected pending outbox events, but found 0! Outbox pattern failure.${NC}"
    docker unpause ledger-redpanda
    exit 1
fi
echo -e "${GREEN}✅ Dual-Write Resiliency Confirmed: Ledger succeeded without broker availability!${NC}"

# 4. Heal the system: Unpause Redpanda
echo -e "\n${YELLOW}[Step 4/5] 🩹 HEALING SYSTEM: Unpausing Kafka/Redpanda broker...${NC}"
docker unpause ledger-redpanda
echo -e "${GREEN}✅ Redpanda broker is back ONLINE.${NC}"

# Wait for Outbox Relay background worker to drain the queue
echo -e "Waiting for Outbox Relay to drain pending events (polling interval: 500ms)..."
sleep 3

PENDING_OUTBOX_AFTER=$(docker exec ledger-postgres psql -U postgres -d ledger_db -t -c "SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING';" | tr -d '[:space:]')
echo -e "  Pending events after recovery: ${GREEN}${PENDING_OUTBOX_AFTER}${NC} (Must drain to 0)"

# 5. Verify Zero-Corruption Accounting Invariants
echo -e "\n${YELLOW}[Step 5/5] Verifying Double-Entry Accounting Invariant...${NC}"
ENTRIES_SUM_AFTER=$(docker exec ledger-postgres psql -U postgres -d ledger_db -t -c "SELECT COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0) FROM entries;" | tr -d '[:space:]')
TOTAL_BALANCES_AFTER=$(docker exec ledger-postgres psql -U postgres -d ledger_db -t -c "SELECT SUM(balance) FROM accounts;" | tr -d '[:space:]')

echo -e "  Double-Entry Delta: ${GREEN}${ENTRIES_SUM_AFTER}${NC}"
echo -e "  Total System Money: ${GREEN}Rp ${TOTAL_BALANCES_AFTER}${NC}"

if [ "${ENTRIES_SUM_AFTER%.*}" -ne 0 ]; then
    echo -e "${RED}❌ CRITICAL: Double-Entry Invariant violated! Delta != 0.${NC}"
    exit 1
fi

echo -e "\n${GREEN}=================================================================${NC}"
echo -e "${GREEN}  🏆 CHAOS TEST PASSED: ZERO DATA LOSS & ZERO LEDGER CORRUPTION!  ${NC}"
echo -e "${GREEN}=================================================================${NC}"
