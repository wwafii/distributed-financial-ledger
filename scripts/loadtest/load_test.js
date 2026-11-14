import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom Metrics
const successfulTransactions = new Counter('successful_transactions');
const failedTransactions = new Counter('failed_transactions');
const transactionLatency = new Trend('transaction_duration_ms');
const errorRate = new Rate('error_rate');

export const options = {
  scenarios: {
    // 1. Warm-up phase
    warmup: {
      executor: 'constant-vus',
      vus: 10,
      duration: '10s',
      startTime: '0s',
    },
    // 2. High Throughput Ramp-up: 50 -> 250 -> 500 VUs
    stress_test: {
      executor: 'ramping-vus',
      startVUs: 20,
      stages: [
        { duration: '20s', target: 100 },
        { duration: '30s', target: 250 },
        { duration: '20s', target: 500 },
        { duration: '20s', target: 100 },
        { duration: '10s', target: 0 },
      ],
      startTime: '10s',
    },
  },
  thresholds: {
    // 95% of requests must complete under 150ms
    http_req_duration: ['p(95)<150', 'p(99)<300'],
    // Error rate must be strictly under 2%
    error_rate: ['rate<0.02'],
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:8000';

const ACCOUNTS = [
  'ACC-ALICE-01',
  'ACC-BOB-02',
  'ACC-CHARLIE-03',
];

export function setup() {
  const tokens = {};
  for (const acc of ACCOUNTS) {
    const res = http.post(`${BASE_URL}/api/v1/auth/token`, JSON.stringify({ account_number: acc }), {
      headers: { 'Content-Type': 'application/json' },
    });
    if (res.status === 200) {
      try {
        tokens[acc] = res.json().data.token;
      } catch (e) {}
    }
  }
  return { tokens };
}

function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export default function (data) {
  // Pick random distinct source and destination
  const srcIdx = Math.floor(Math.random() * ACCOUNTS.length);
  let destIdx = Math.floor(Math.random() * ACCOUNTS.length);
  while (destIdx === srcIdx) {
    destIdx = Math.floor(Math.random() * ACCOUNTS.length);
  }

  const sourceAccount = ACCOUNTS[srcIdx];
  const destAccount = ACCOUNTS[destIdx];
  const amount = (Math.floor(Math.random() * 50) + 1) * 1000; // Rp 1.000 - Rp 50.000

  const idempotencyKey = generateUUID();
  const payload = JSON.stringify({
    source_account: sourceAccount,
    destination_account: destAccount,
    amount: amount,
    currency: 'IDR',
    description: `Load test transfer from ${sourceAccount} to ${destAccount}`,
  });

  const headers = {
    'Content-Type': 'application/json',
    'Idempotency-Key': idempotencyKey,
  };
  if (data && data.tokens && data.tokens[sourceAccount]) {
    headers['Authorization'] = `Bearer ${data.tokens[sourceAccount]}`;
  }

  const start = Date.now();
  const res = http.post(`${BASE_URL}/api/v1/transactions`, payload, { headers });
  const latency = Date.now() - start;

  transactionLatency.add(latency);

  const isSuccess = check(res, {
    'status is 200 or 201': (r) => r.status === 200 || r.status === 201,
    'has transaction_id': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.data && body.data.transaction_id !== undefined;
      } catch (e) {
        return false;
      }
    },
  });

  if (isSuccess) {
    successfulTransactions.add(1);
    errorRate.add(0);
  } else {
    failedTransactions.add(1);
    errorRate.add(1);
  }

  // Small pacing between iterations
  sleep(0.05);
}
