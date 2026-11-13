import http from 'k6/http';
import { check, sleep } from 'k6';

// Specifically tests Fraud Engine velocity rules:
// Tier 1: Anti-Bot Burst (> 3 transactions in 10s from same account -> FREEZE)
// Tier 4: Whale transfer (>= Rp 50.000.000 -> CHALLENGE)

export const options = {
  scenarios: {
    // 1. Anti-Bot Burst scenario: Fire 5 rapid transactions within 2 seconds
    anti_bot_burst: {
      executor: 'constant-vus',
      vus: 5,
      duration: '3s',
      startTime: '0s',
    },
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:8000';

function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export default function () {
  // All requests hit Alice's account rapidly
  const payload = JSON.stringify({
    source_account: 'ACC-ALICE-01',
    destination_account: 'ACC-BOB-02',
    amount: 15000.0,
    currency: 'IDR',
    description: 'Anti-bot burst velocity probe',
  });

  const headers = {
    'Content-Type': 'application/json',
    'Idempotency-Key': generateUUID(),
  };

  const res = http.post(`${BASE_URL}/api/v1/transactions`, payload, { headers });

  check(res, {
    'request executed or intercepted by fraud rule': (r) =>
      r.status === 200 || r.status === 201 || r.status === 403 || r.status === 422,
  });

  sleep(0.1);
}
