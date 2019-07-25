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
   * @param   {string}   options.timeout         job will run after this time (in case of too long previous tasks processing)
   * @memberof Coordinator
   */
  constructor({jobId, client, concurrency, keyPending, keyProcessing, timeout} = {}) {
    this._jobId = jobId;
    this._keepAliveTimeout = null;
    this._concurrency = concurrency;
    this._client = client;
    this._resolve = null;
    this._keyPending = keyPending;
    this._keyProcessing = keyProcessing;
    this._timeout = timeout;
    this._startTime = null;
    this._canRun = new Promise(resolve => this._resolve = resolve);
  }

  get canRun() {
    return this._canRun;
  }

  /**
   * Keep lock key alive
   *
   * @param   {string}   lockKey
   * @param   {number}   ms
   * @memberof Coordinator
   */
  async keepAlive(lockKey, ms) {
    this.stopKeepAlive();
    await this._client.setex(lockKey, ms * 2 / 1000, '').catch(() => null);
    this._keepAliveTimeout = setTimeout(() => this.keepAlive(lockKey, ms), ms);
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
   * Check if job can run and fulfill canRun promise
   *
   * @memberof Coordinator
   */
  async setCanRun() {
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

    if (processingCount < this._concurrency && nextJobId === this._jobId) {
      this._resolve();
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
        const existing = await this._client.exists(`lock:${queueKey}:${jobId}`);

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
