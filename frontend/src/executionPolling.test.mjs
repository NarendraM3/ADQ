import assert from 'node:assert/strict';
import { pollExecution } from './executionPolling.mjs';

const tick = () => new Promise(resolve => setImmediate(resolve));
const noWait = async () => {};

// Poll the exact ID through queued and running states, then apply counts/details.
{
  const ids = [], updates = [];
  const states = [
    { id: 42, status: 'queued', passed_count: 0, failed_count: 0 },
    { id: 42, status: 'running', passed_count: 0, failed_count: 0 },
    { id: 42, status: 'succeeded', passed_count: 8, failed_count: 2 },
  ];
  pollExecution({ executionId: 42, getExecution: async id => { ids.push(id); return states.shift(); },
    getFailed: async id => { ids.push(id); return [{ row_index: 4, record: { EmployeeID: 'E5' }, failure_reasons: ['Email invalid'] }]; },
    onUpdate: (run, details) => updates.push({ run, details }), onError: error => { throw error; }, delay: noWait });
  await tick(); await tick(); await tick();
  assert.deepEqual(ids, [42, 42, 42, 42]);
  assert.equal(updates.at(-1).run.passed_count + updates.at(-1).run.failed_count, 10);
  assert.equal(updates.at(-1).details[0].failure_reasons[0], 'Email invalid');
}

// Polling continues beyond 20 reads until a real terminal state.
{
  let count = 0, final;
  pollExecution({ executionId: 7, getExecution: async id => { assert.equal(id, 7); count++; return { id, status: count < 25 ? 'running' : 'incomplete' }; },
    getFailed: async () => [], onUpdate: run => { final = run; }, onError: error => { throw error; }, delay: noWait });
  for (let i = 0; i < 30 && final?.status !== 'incomplete'; i++) await tick();
  assert.equal(count, 25);
  assert.equal(final.status, 'incomplete');
}

// A request that resolves after a newer execution becomes current cannot update UI.
{
  let resolveOld, current = true, updates = 0;
  pollExecution({ executionId: 8, getExecution: () => new Promise(resolve => { resolveOld = resolve; }),
    getFailed: async () => [], onUpdate: () => updates++, onError: () => {}, isCurrent: () => current });
  await tick(); current = false;
  resolveOld({ id: 8, status: 'succeeded', passed_count: 100, failed_count: 0 });
  await tick();
  assert.equal(updates, 0);
}

// Backend and artifact errors are surfaced; transient status failures remain retryable.
{
  const errors = []; let attempts = 0, done = false;
  pollExecution({ executionId: 9, getExecution: async id => { assert.equal(id, 9); if (!attempts++) throw Error('temporary timeout'); return { id, status: 'failed', error: 'Databricks write failed' }; },
    getFailed: async () => { throw Error('no saved outputs'); }, onUpdate: run => { if (run.status === 'failed') done = true; },
    onError: (error, details) => errors.push([error.message, details]), delay: noWait });
  for (let i = 0; i < 8 && errors.length < 2; i++) await tick();
  assert.equal(done, true);
  assert.deepEqual(errors.map(x => x[0]), ['temporary timeout', 'no saved outputs']);
  assert.deepEqual(errors.map(x => x[1]), [false, true]);
}

// A deleted or invalid execution ID stops polling and exposes the API error.
{
  let attempts = 0, message;
  pollExecution({ executionId: 404, getExecution: async () => { attempts++; const error = Error('Execution not found'); error.status = 404; throw error; },
    getFailed: async () => [], onUpdate: () => {}, onError: error => { message = error.message; }, delay: noWait });
  await tick();
  assert.equal(attempts, 1);
  assert.equal(message, 'Execution not found');
}
