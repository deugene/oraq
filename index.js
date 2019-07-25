'use strict';

const Redis = require('ioredis');
const crypto = require('crypto');
const {promisify} = require('util');
const randomBytes = promisify(crypto.randomBytes);
const {name: packageName} = require('./package.json');
const Coordinator = require('./Coordinator');

const DEFAULT_ID = 'queue';

/**
 * Class representing an ordered redis asynchronous queue
 */
class Oraq {
  /**
   * Creates an instance of Oraq
   *
   * @param   {object}   options
   * @param   {string}   options.prefix        custom redis key prefix
   * @param   {string}   options.id            id (limiters with the same prefix and id share their queues)
   * @param   {*}        options.connection    redis connection param
   * @param   {number}   options.concurrency   jobs concurrency
   * @param   {number}   options.ping          processing job keep alive interval
   * @param   {number}   options.timeout       job will run after this time (in case of too long previous tasks processing)
   * @memberof Oraq
   */
  constructor(options) {
    const {
      id = DEFAULT_ID,
      prefix = packageName,
      connection,
      ping = 60 * 1000,              // 1 minute
      timeout = 2 * 60 * 60 * 1000,  // 2 hours
      concurrency = 1
    } = options || {};

    this._ping = ping;
    this._timeout = timeout;
    this._concurrency = concurrency;
    // keys for data store
    this._key = [prefix, id].join(':');
    this._keyProcessing = this._key + ':processing';
    this._keyPending = this._key + ':pending';
    // redis client
    this._client = new Redis(connection);
    // enable namespace events
    this._client.config('SET', 'notify-keyspace-events', 'Kgxl');
    // keyspace events subscriber
    this._subscriber = new Redis(connection);
    this._ready = null;
  }

  /**
   * Initialize
   *
   * @private
   * @memberof Oraq
   */
  async _init() {
    if (this._ready === null) {
      this._ready = new Promise((resolve, reject) => {
        this._subscriber.psubscribe(`__keyspace@0__:${this._key}:*`, err => {
          if (err) {
            reject(err);
          }

          this._subscriber.psubscribe(`__keyspace@0__:lock:${this._key}:*`, err => {
            if (err) {
              reject(err);
            } else {
              resolve();
            }
          });
        });
      });
    }

    return this._ready;
  }

  /**
   * Limit jobs rate
   *
   * @param   {function}  job       job to process
   * @param   {object}    options
   * @param   {string}    options.jobId
   * @param   {*}         options.jobData   data to pass to the job
   * @param   {boolean}   options.lifo      last-in-first-out (fifo by default)
   * @memberof Oraq
   */
  async limit(job, {jobId, jobData, lifo = false} = {}) {
    if (!this._isSubscribed) {
      await this._init();
    }

    jobId = jobId || (await randomBytes(16)).toString('hex');

    const coordinator = new Coordinator({
      jobId,
      client: this._client,
      concurrency: this._concurrency,
      keyPending: this._keyPending,
      keyProcessing: this._keyProcessing,
      timeout: this._timeout
    });
    const onKeyEvent = this._getOnKeyEvent(coordinator);

    try {
      let result;

      // add job to the pending queue
      await this._client
        .multi()
        .setex(`lock:${this._keyPending}:${jobId}`, this._timeout * 2 / 1000, '')
        [lifo ? 'rpush' : 'lpush'](this._keyPending, jobId)  // eslint-disable-line no-unexpected-multiline
        .exec();
      // listen processing key events
      this._subscriber.addListener('pmessage', onKeyEvent);
      // concurrency
      await coordinator.setCanRun();
      await coordinator.canRun;
      this._subscriber.removeListener('pmessage', onKeyEvent);
      // create lock key and keep it alive
      await coordinator.keepAlive(`lock:${this._keyProcessing}:${jobId}`, this._ping);
      // move job from pending to processing queue
      await this._client
        .multi()
        .brpoplpush(this._keyPending, this._keyProcessing, 0)
        .del(`lock:${this._keyPending}:${jobId}`)
        .exec();
      // run job
      result = await job(jobData);

      return result;
    } finally {
      // stop all timers and remove all listeners
      coordinator.stopKeepAlive();
      this._subscriber.removeListener('pmessage', onKeyEvent);
      // remove processing job id and lock key
      await this._client
        .multi()
        .lrem(this._keyProcessing, 0, jobId)
        .del(`lock:${this._keyProcessing}:${jobId}`)
        .exec();
    }
  }

  /**
   * Get on key event listener
   *
   * @param   {object}   coordinator
   * @returns {function}
   * @private
   * @memberof Oraq
   */
  _getOnKeyEvent(coordinator) {
    return (pattern, channel, message) => {
      const lockStart = '__keyspace@0__:lock:';
      const queueStart = `__keyspace@0__:${this._key}`;

      if (message === 'expired' && channel.startsWith(lockStart)) {
        for (const queueKey of [this._keyPending, this._keyProcessing]) {
          const start = `${lockStart}${queueKey}:`;

          if (channel.startsWith(start)) {
            const expiredJobId = channel.slice(start.length);

            this._client.lrem(queueKey, 0, expiredJobId)
              .then(() => coordinator.setCanRun())
              .catch(console.error);
          }
        }
      } else if (['rpop', 'lrem'].includes(message) && channel.startsWith(queueStart)) {
        coordinator.setCanRun().catch(console.error);
      }
    };
  }

  /**
   * Close redis connections
   *
   * @param   {object}   coordinator
   * @returns {function}
   * @private
   * @memberof Oraq
   */
  async quit() {
    await this._subscriber.quit();
    await this._client.quit();
  }
}


module.exports = Oraq;
module.exports.default = Oraq;
