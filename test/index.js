'use strict';

const assert = require('assert');
const Oraq = require('../index');
const Coordinator = require('../Coordinator');


/**
 *
 * Coordinator tests
 *
 */

/**
 * options validation
 */
(() => {
  assert.throws(() => new Coordinator(), /^Error: options are required/);
  assert.throws(() => new Coordinator({}), /^Error: jobId/);
  assert.throws(() => new Coordinator({
    jobId: '1'
  }), /^Error: keyPending/);
  assert.throws(() => new Coordinator({
    jobId: '1',
    keyPending: 'pending'
  }), /^Error: keyProcessing/);
  assert.throws(() => new Coordinator({
    jobId: '1',
    keyPending: 'pending',
    keyProcessing: 'processing'
  }), /^Error: lock/);
  assert.throws(() => new Coordinator({
    jobId: '1',
    keyPending: 'pending',
    keyProcessing: 'processing',
    lock: ':lock'
  }), /^Error: concurrency/);
  assert.throws(() => new Coordinator({
    jobId: '1',
    keyPending: 'pending',
    keyProcessing: 'processing',
    lock: ':lock',
    concurrency: 1
  }), /^Error: timeout/);
  assert.throws(() => new Coordinator({
    jobId: '1',
    keyPending: 'pending',
    keyProcessing: 'processing',
    lock: ':lock',
    concurrency: 1,
    timeout: 10000
  }), /^Error: client/);
})();

/**
 * properties
 */

(() => {
  const coordinator = new Coordinator({
    jobId: '1',
    keyPending: 'pending',
    keyProcessing: 'processing',
    lock: ':lock',
    concurrency: 1,
    timeout: 10000,
    client: {}
  });

  assert.ok(coordinator);
  assert.ok(coordinator.canRun, true);
  assert.strictEqual(typeof coordinator.keepAlive, 'function');
  assert.strictEqual(typeof coordinator.stopKeepAlive, 'function');
  assert.strictEqual(typeof coordinator.wait, 'function');
  assert.strictEqual(typeof coordinator.stopWait, 'function');
})();


/**
 *
 * Oraq tests
 *
 */

Promise.resolve().then(async () => {
  /**
   * properties
   */

  const oraq = new Oraq();

  try {
    assert.ok(oraq);
    assert.strictEqual(typeof oraq.limit, 'function');
    assert.strictEqual(typeof oraq.quit, 'function');
    assert.strictEqual(typeof oraq.removeJobById, 'function');
  } finally {
    await oraq.quit();
  }
}).then(async () => {
  /**
   * processing time
   */

  const job = ms => new Promise(resolve => setTimeout(() => resolve(ms), ms));
  const oraq = new Oraq({
    id: 'test',
    prefix: 'oraq_test',
    concurrency: 1
  });

  try {
    const start = Date.now();

    await Promise.all([
      1000,
      1000,
      1000,
      1000
    ].map(delay => oraq.limit(job, {jobData: delay})));

    const end = Date.now();

    assert.strictEqual(end >= start + 4000, true);
  } finally {
    // close redis connections
    await oraq.quit();
  }
});
