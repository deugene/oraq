'use strict';

const assert = require('assert');
const Oraq = require('../index');
const Coordinator = require('../Coordinator');

/**
 * Oraq tests
 */

const oraq = new Oraq();

assert.ok(oraq);
assert.strictEqual(typeof oraq.limit, 'function');
assert.strictEqual(typeof oraq.quit, 'function');
assert.strictEqual(typeof oraq.removeJobById, 'function');

/**
 * Coordinator tests
 */

const coordinator = new Coordinator();

assert.ok(coordinator);
assert.ok(coordinator.canRun, true);
assert.strictEqual(typeof coordinator.keepAlive, 'function');
assert.strictEqual(typeof coordinator.stopKeepAlive, 'function');
assert.strictEqual(typeof coordinator.setCanRun, 'function');

// close redis connections
oraq.quit();
