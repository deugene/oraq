'use strict';

/**
 * Class representing a coordinator for Oraq
 */
class Coordinator {
  /**
   * Creates an instance of Coordinator
   *
   * @param   {object}   options
   * @param   {string}   options.jobId
   * @param   {object}   options.client          redis client
   * @param   {number}   options.concurrency     jobs concurrency
   * @param   {string}   options.keyPending      pending queue key
   * @param   {string}   options.keyProcessing   processing queue key
   * @param   {number}   options.timeout         job will run after this time (in case of too long previous tasks processing)
   * @param   {string}   options.mode            mode {string} ('limiter' - rate limiter (no order guarantee); 'queue' - real queue (keep order))
   * @memberof Coordinator
   */
  constructor(options) {
    this._validate(options);

    const {jobId, client, concurrency, keyPending, keyProcessing, timeout, lock, mode} = options;

    this._jobId = jobId;
    this._keepAliveTimeout = null;
    this._waitTimeout = null;
    this._concurrency = concurrency;
    this._client = client;
    this._resolve = null;
    this._keyPending = keyPending;
    this._keyProcessing = keyProcessing;
    this._timeout = timeout;
    this._lock = lock;
    this._mode = mode;
    this._startTime = null;
    this._canRun = new Promise(resolve => this._resolve = resolve);
  }

  /**
   * Validate options
   *
   * @param   {object}   options
   * @private
   * @memberof Coordinator
   */
  _validate(options) {
    if (!options) {
      throw new Error('options are required');
    }
    if (typeof options.jobId !== 'string') {
      throw new Error('jobId must be a string');
    }
    if (typeof options.keyPending !== 'string') {
      throw new Error('keyPending must be a string');
    }
    if (typeof options.keyProcessing !== 'string') {
      throw new Error('keyProcessing must be a string');
    }
    if (typeof options.lock !== 'string') {
      throw new Error('lock must be a string');
    }
    if (!Number.isInteger(options.concurrency)) {
      throw new Error('concurrency must be an integer');
    }
    if (!Number.isInteger(options.timeout)) {
      throw new Error('timeout must be an integer');
    }
    if (!options.client) {
      throw new Error('client is required');
    }
  }

  get canRun() {
    return this._canRun;
  }

  /**
   * Keep lock key alive
   *
   * @param   {number}   ms
   * @memberof Coordinator
   */
  async keepAlive(ms) {
    const lockKey = `${this._keyProcessing}:${this._jobId}${this._lock}`;

    this.stopKeepAlive();
    await this._client.setex(lockKey, ms * 2 / 1000, '').catch(() => null);
    this._keepAliveTimeout = setTimeout(() => this.keepAlive(ms), ms);
  }

  /**
   * Clear keep alive timeout
   *
   * @memberof Coordinator
   */
  stopKeepAlive() {
    if (this._keepAliveTimeout) {
      clearTimeout(this._keepAliveTimeout);
      this._keepAliveTimeout = null;
    }
  }

  /**
   * Wait for queue changes
   *
   * @param   {number}   ms
   * @memberof Coordinator
   */
  async wait(ms) {
    this.stopWait();
    await this._setCanRun();
    this._waitTimeout = setTimeout(() => this.wait(ms), ms);
  }

  /**
   * Clear wait timeout
   *
   * @memberof Coordinator
   */
  stopWait() {
    if (this._waitTimeout) {
      clearTimeout(this._waitTimeout);
      this._waitTimeout = null;
    }
  }

  /**
   * Check if job can run and fulfill canRun promise
   *
   * @private
   * @memberof Coordinator
   */
  async _setCanRun() {
    if (!this._startTime) {
      this._startTime = Date.now();
    }
    if (Date.now() - this._startTime > this._timeout) {
      this._resolve();
      return;
    }

    await this._removeStuckJobs(this._keyPending);
    await this._removeStuckJobs(this._keyProcessing);

    const [llenRes, lindexRes] = await this._client
      .multi()
      .llen(this._keyProcessing)
      .lindex(this._keyPending, -1)
      .exec();
    const processingCount = llenRes[1];
    const nextJobId = lindexRes[1];

    if (processingCount < this._concurrency) {
      switch (this._mode) {
        case 'limiter':
          this._resolve();
          break;
        case 'queue':
          nextJobId === this._jobId && this._resolve();
          break;
      }
    }
  }

  /**
   * Remove stuck jobs
   *
   * @param   {string}   queueKey
   * @returns {Promise}
   * @private
   * @memberof Coordinator
   */
  async _removeStuckJobs(queueKey) {
    const jobIds = await this._client.lrange(queueKey, 0, -1);
    let stuckJobs = new Set();

    if (!jobIds.length) {
      return;
    }

    for (const jobId of jobIds) {
      if (jobId) {
        const existing = await this._client.exists(`${queueKey}:${jobId}${this._lock}`);

        if (!existing) {
          stuckJobs.add(jobId);
        }
      }
    }

    if (!stuckJobs.size) {
      return;
    }

    const pipe = this._client.multi();

    for (const jobId of stuckJobs) {
      pipe.lrem(queueKey, 0, jobId);
    }

    return pipe.exec().catch(() => null);
  }
}

module.exports = Coordinator;
