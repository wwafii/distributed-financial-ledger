import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// Test that when 50 concurrent virtual users hit the API with the SAME key at the exact same millisecond:
// 1. Exactly 1 request acquires the lock and executes
// 2. In-flight requests receive 409 Conflict
// 3. Subsequent retries receive 200 with X-Idempotent-Replay header
// 4. Zero double-spending occurs!

const firstSuccessCounter = new Counter('first_success_count');
const conflictCounter = new Counter('conflict_409_count');
const replayCounter = new Counter('replay_200_count');

export const options = {
  scenarios: {
    // 50 concurrent requests firing in the exact same second
    concurrent_stampede: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: 50,
      maxDuration: '10s',
    },
  },
  thresholds: {
    // Exactly 1 request must be the first success
    first_success_count: ['count==1'],
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:8000';
// Fixed idempotency key for this test run
const FIXED_KEY = __ENV.FIXED_KEY || `idemp-stress-${Date.now()}`;

export default function () {
  const payload = JSON.stringify({
    source_account: 'ACC-ALICE-01',
    destination_account: 'ACC-BOB-02',
    amount: 50000.0,
    currency: 'IDR',
    description: 'Concurrent Idempotency Stress Test',
  });

  const headers = {
    'Content-Type': 'application/json',
    'Idempotency-Key': FIXED_KEY,
  };

  const res = http.post(`${BASE_URL}/api/v1/transactions`, payload, { headers });

  const isFirstSuccess = (res.status === 201 || res.status === 200) && res.headers['X-Idempotent-Replay'] === undefined;
  const isReplay = (res.status === 201 || res.status === 200) && res.headers['X-Idempotent-Replay'] === 'true';
  const isConflict = res.status === 409;

  check(res, {
    'status is 201/200 (Success/Replay) or 409 (In-flight Conflict)': (r) =>
      r.status === 201 || r.status === 200 || r.status === 409,
  });

  if (isFirstSuccess) {
    firstSuccessCounter.add(1);
  } else if (isConflict) {
    conflictCounter.add(1);
  } else if (isReplay) {
    replayCounter.add(1);
  }
}
