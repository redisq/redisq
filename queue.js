"use strict";

const { customAlphabet } = require('nanoid');
const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
const taskID = customAlphabet(alphabet, 32);

const Redis = require('ioredis');

const NAMESPACE = 'QUEUE';
const DEDUPESET = 'DEDUPE';

const sleep = delay => new Promise(resolve => setTimeout(() => resolve(), delay));

const prettyError = (error) => {
    let keys = error ? Reflect.ownKeys(error) : [];

    let result = keys.reduce((memo, key) => {
        memo[key] = error[key];

        return memo;
    }, {});

    process.env.NODE_ENV === 'production' && delete result.stack;
    delete result.ctx;

    return result;
};

class RedisStreams {
    constructor({ 
        redis = new Redis(process.env.REDIS), 
        length = 1000000,
        logger, 
        worker = () => void 0, 
        batch_size = 10,
        batch_sync = true,
        reclaim_interval = 0,
        loop_interval = 5000,
        delete_event = true,
        onComplete = () => void 0,
     } = {}) {
        this.streams = {};

        this.redis = redis;
        this.length = length;
        this.logger = logger || {
            info() {},
            log() {},
            error() {},
            debug() {},
            fatal() {},
            trace() {},
            warn() {}
        };
        /* this.concurency = concurency; */ // CONCYRENCY CAN BE MADE BY CREATING MULTIPLY INSTANCES WITH THE SAME NAMES
        this.batch_size = batch_size;
        this.batch_sync = batch_sync;
        this.delete_event = delete_event;

        this.worker = worker;
        this.onComplete = onComplete;

        this.started = false;
        this.ID = 0;

        this.loop_interval = loop_interval;
        this.reclaim_interval = reclaim_interval;

        if(this.reclaim_interval) {
            if(this.reclaim_interval < 1000) {
                this.reclaim_interval = 1000;
            }

            setInterval(() => {
                this.reclaim();
            }, this.reclaim_interval);
        }
    }

    async reclaim({ stream }) {
        const { pending, groups } = await this.size({ stream });

        if(pending) {
            for(const group of groups) {
                if(group.pending) {
                    const events = await this.redis.xpending(stream, group.name, '-', '+', this.batch_size);

                    for(const event of events) {
                        const [id, consumer, /*idle, delivered */] = event;

                        const [[message_id, entries] = []] = await this.redis.xrange(stream, id, id);

                        if(message_id) {
                            const data = [[stream, [[message_id, entries]]]];
        
                            this._process({ stream, group: group.name, consumer, data });
                        }
                        else {
                            await this.redis
                                .multi()
                                .xack(stream, group.name, id)
                                //.xdel(stream, id)
                                .exec();
                        }
                    }
                }
            }
        }
    }

    clear({ stream }) {
        return this.redis.multi()
            .del(stream)
            .del(`${stream}:${DEDUPESET}`)
            .xgroup('CREATE', stream, 'NEO', 0, 'MKSTREAM')
            .xgroup('DESTROY', stream, 'NEO')
            .exec();
    }

    async list() {
        let streams = [];
        let cursor = '0';

        while(cursor) {
            let keys = [];

            [cursor, keys] = await this.redis.scan(cursor, 'TYPE', 'stream');
            
            cursor = +cursor;

            streams = [...streams, ...keys];
        }

        return streams;
    }

    async size({ stream }) {
        const [[, size], [, groups = []]] = await this.redis.multi()
            .xlen(stream)
            .xinfo('GROUPS', stream)
            .exec();

        const info = groups.reduce((memo, group) => {
            const entries = [];

            while (group.length) {
                const [key, value] = group.splice(0, 2);

                entries.push([key, value]);
            }

            group = Object.fromEntries(entries);
            //const [, , , , , pending] = group;

            memo.pending += group.pending
            memo.groups.push(group);

            return memo;
        }, { pending: 0, groups: [] });

        return { size, ...info };
    }

    async push({ stream, id = taskID(), event, dedupe = true }) {
        const { schema, payload } = event;
        const { options } = schema;

        const exists = dedupe ? !await this.redis.sadd(`${stream}:${DEDUPESET}`, id) : false;

        if(!exists) {
            payload.$system = { schema: { name: schema.name, version: schema.version }, created: Date.now() };

            return this.redis.xadd(stream, '*', 'payload', JSON.stringify(payload), 'options', JSON.stringify(options), 'id', id);
            //return this.redis.xadd(stream, /* 'MAXLEN', '~', this.length,  */'*', 'payload', JSON.stringify(payload), 'options', JSON.stringify(options), 'id', id);
        }

        //await this.redis.srem(`${stream}:${DEDUPESET}`, id);

        return false;
    }

    async _process({ stream, group, consumer, data }) {
        data = data || [[]];

        const [[, messages = []]] = data;

        for(const message of messages) {
            const [message_id, entries] = message;

            if(!entries) {
                await this.redis.xack(stream, group, message_id);

                continue;
            }

            let [, payload, , options, , task_id, , errors] = entries || [];

            try {
                payload = JSON.parse(payload);

                options = options ? JSON.parse(options) : {};
                errors = errors ? JSON.parse(errors) : 0;
            }
            catch {
            }

            const { $system: { created, schema }, ...rest } = payload;
            const event = { id: task_id, schema: { ...schema, options }, payload: rest, created };

            const worker = ({ stream, group, consumer, message_id, event, errors }) => {
                return new Promise((resolve, reject) => {
                    if(options.timeout) {
                        const timer = setTimeout(() => {
                            clearTimeout(timer);

                            reject({
                                code: 500,
                                message: `Task timed out after ${options.timeout} ms.`,
                            });
                        }, options.timeout);
                    }

                    const worker = new Promise(resolve => {
                        resolve(this.worker({ stream, group, consumer, message_id, event, errors }));
                        //resolve(this.worker({ stream, group, consumer, message_id, payload, task_id, options, errors }));
                    });

                    worker.then(result => {
                        resolve(result);
                    }).catch(error => {
                        reject(error)
                    });
                });
            };

            let retries = options.retries;

            await worker({ stream, group, consumer, message_id, event, errors }).then(async result => {
                if (result === false) {
                    const filtered = [[stream, [[message_id, entries]]]];

                    if(options.delay) {
                        sleep(options.delay).then(() => {
                            this._process({ stream, group, consumer, data: filtered });
                        });
                    }
                    else {
                        this._process({ stream, group, consumer, data: filtered });
                    }

                }
                else {
                    const [, , [, size], , [, [pending]]] = await this.redis
                        .multi()
                        .xack(stream, group, message_id)
                        .srem(`${stream}:${DEDUPESET}`, task_id)
                        .xlen(stream)
                        .xgroup('CREATE', stream, group, 0)
                        .xpending(stream, group)
                        .xdel(stream, this.delete_event ? message_id : '')
                        .exec();

                    const calculated_size = this.delete_event ? size - 1 : size;

                    this.onComplete({ state: 'COMPLETE', result, stream, group, consumer, message_id, event, errors, size: calculated_size, pending });
                }
            }).catch(async (error) => {
                error = prettyError(error);
                
                errors++;
                
                const retry = retries > 0 ? retries - errors  : 0;

                if (retry > 0) {
                    entries.includes('errors') && entries.splice(-2);
                    entries.push('errors');
                    entries.push(errors);

                    const filtered = [[stream, [[message_id, entries]]]];

                    if(options.delay) {
                        sleep(options.delay).then(() => {
                            this._process({ stream, group, consumer, data: filtered });
                        });
                    }
                    else {
                        this._process({ stream, group, consumer, data: filtered });
                    }

                } 
                else {
                    const [, , [, size], , [, [pending]]] = await this.redis
                        .multi()
                        .xack(stream, group, message_id)
                        .srem(`${stream}:${DEDUPESET}`, task_id)
                        .xlen(stream)
                        .xgroup('CREATE', stream, group, 0)
                        .xpending(stream, group)
                        .xdel(stream, this.delete_event ? message_id : '')
                        .exec();

                    const calculated_size = this.delete_event ? size - 1 : size;

                    this.onComplete({ state: 'ERROR', error, stream, group, consumer, message_id, event, errors, size: calculated_size, pending });
                }
            });
        }
    }

    async go({ stream, group, consumer, id: pointer, timeout }) {

        while(this.streams[`${stream}.${group}.${consumer}`]) {

            await this.redis.xreadgroup('GROUP', group, consumer, 'COUNT', this.batch_size, 'STREAMS', stream, pointer).then(async (data) => {
                pointer = '>';

                if(this.batch_sync) {
                    await this._process({ stream, group, consumer, data });
                }
                else {
                    this._process({ stream, group, consumer, data });
                }
            }).catch(error => {
                this.redis.xgroup('CREATE', stream, group, 0);
            });

            await sleep(timeout || this.loop_interval);
        }
    }

    async listen({ stream, group, consumer, id = 0, timeout }) {
        const start = () => {
            this.go({ stream, group, consumer, id, timeout });

            return () => this.streams[`${stream}.${group}.${consumer}`] = false;
        };

        this.streams[`${stream}.${group}.${consumer}`] = true;

        return this.redis.xgroup('CREATE', stream, group, 0, 'MKSTREAM')
            .then(() => {
                return start();
            })
            .catch(error => {
                return start();
            });

    }
}


const COMMANDS = {
    xadd({ redis, stream, length, payload, options, id }) {
        return redis.xadd(stream, 'MAXLEN', '~', length, '*', 'payload', JSON.stringify(payload), 'options', JSON.stringify(options), 'id', id);
    }
}

class RedisStreamsQueue {
    constructor({ 
        name = 'queue', 
        group, 
        consumer, 
        redis = new Redis(process.env.REDIS), 
        length = 1000000,
        logger, 
        //logger = console, 
        worker = async () => void 0, 
        batch_size = 10,
        batch_sync = true,
        claim_interval = 1000 * 60 * 60,
        loop_interval = 5000,
        onTaskComplete,
        onTaskError,
        onTaskFinal,
        onTaskPending,
        onClaim
     }) {
        this.name = name;
        this.group = group || `${name}:group`;
        
        this.consumer = consumer || `consumer:${taskID()}`;

        this.redis = redis;
        this.length = length;
        this.logger = logger || {
            info() {},
            log() {},
            error() {},
            debug() {},
            fatal() {},
            trace() {},
            warn() {}
        };
        /* this.concurency = concurency; */ // CONCYRENCY CAN BE MADE BY CREATING MULTIPLY INSTANCES WITH THE SAME NAMES
        this.batch_size = batch_size;
        this.batch_sync = batch_sync;

        this.worker = worker;
        this.onTaskComplete = onTaskComplete;
        this.onTaskError = onTaskError;
        this.onTaskFinal = onTaskFinal;
        this.onTaskPending = onTaskPending;
        this.onClaim = onClaim;

        this.redis.xgroup('CREATE', name, this.group, 0, 'MKSTREAM').catch(() => void 0);

        this.started = false;
        this.ID = 0;

        this.loop_interval = loop_interval;
        this.claim_interval = claim_interval;

        this.claim_interval && setInterval(() => {
            this.claim();
        }, this.claim_interval);

        this.claim_interval && this.claim();
    }

    async claim({ force = false } = {}) {
        let consumers = await this.redis.xinfo('CONSUMERS', this.name, this.group);

        consumers = consumers.reduce((memo, consumer) => {
            let [, name,, pending,, idle] = consumer;

            memo.push({ name, pending, idle });

            return memo;
        }, []);

        const expired = force ? consumers : consumers.filter(consumer => consumer.idle > this.claim_interval);

        for(let consumer of expired) {
            if(consumer.name !== this.consumer && consumer.pending > 0) {
                let pending = await this.redis.xpending(this.name, this.group, '-', '+', this.batch_size, consumer.name);

                pending = pending.map(task => task[0]);

                let tasks = {};

                for(let id of pending) {
                    let range = await this.redis.xrange(this.name, id, id);

                    if(range.length) {
                        let [[, body]] = range;
                        let [, , , , , task_id] = body;

                        tasks[task_id] = id;
                    }
                }

                const claims = Object.values(tasks);

                pending = await this.redis.xclaim(this.name, this.group, this.consumer, 0, ...claims, 'JUSTID');

                pending.length && this.onClaim && this.onClaim({ name: this.name, consumer: consumer.name, tasks: pending });

                this.logger.info(`Claimed tasks from ${consumer.name} -> ${this.consumer}: ${pending}`);

                consumer.skip = claims.length > this.batch_size;
            }

            if(consumer.name !== this.consumer && !consumer.skip) {
                const pending_count = await this.redis.xgroup('DELCONSUMER', this.name, this.group, consumer.name);

                if(pending_count > 0) {
                    this.logger.warn(`Consumer ${consumer.name} had pending messages!`);
                }

                this.logger.info(`Consumer deleted: ${consumer.name}`);
            }
        }
    }

    async clearConsumers({ force = false }) {
        let consumers = await this.redis.xinfo('CONSUMERS', this.name, this.group);

        consumers = consumers.reduce((memo, consumer) => {
            let [, name,, pending,, idle] = consumer;

            memo.push({ name, pending, idle });

            return memo;
        }, []);

        for(let consumer of consumers) {
            if(consumer.name !== this.consumer && (!consumer.pending || force)) {
                const pending_count = await this.redis.xgroup('DELCONSUMER', this.name, this.group, consumer.name);

                this.logger.info(`Consumer deleted: ${consumer.name}`);
            }
        }
    }

    async clear(task_id) {
        if(!task_id) {
            await this.redis.xtrim(this.name, 'MAXLEN', 0);

            return this.redis.del(`${this.name}:${DEDUPESET}`);
        }
        else {
            const message_id = await this.redis.get(`${this.name}:TASK:${task_id}`);

            return this.redis.multi()
                .xdel(this.name, message_id)
                .srem(`${this.name}:${DEDUPESET}`, task_id)
                .exec();
        }
    }

    size() {
        return this.redis.xlen(this.name);
    }

    async push({ payload = {}, id = taskID(), options = {} }) {
        options = { timeout: 5000, delay: 0, retries: 0, ...options };

        const exists = !await this.redis.sadd(`${this.name}:${DEDUPESET}`, id); // dedupe

        if(!exists) {
            this.logger.info(`Pushed task:`, this.name, id);

            const message_id = await COMMANDS.xadd({ redis: this.redis, stream: this.name, length: this.length, payload, options, id }).then(async (message_id) => {
                await this.redis.set(`${this.name}:TASK:${id}`, message_id);

                return message_id;
            });

            return message_id;
        }
        else {
            this.logger.info(`Task already exists:`, this.name, id);
        }

        return exists;
    }

    restart() {
        if(this.started) {
            this.ID = 0;
            this.started = false;
            this.start();
        }
        else {
            this.start();
        }
    }

    start() {
        if(!this.started) {
            this.started = true;
            this._loop();
        }
    }

    stop() {
        this.started = false;
    }

    async _loop() {
        while(this.started) {
            let semaphore = false;

            await this.redis.xreadgroup('GROUP', this.group, this.consumer, /* 'BLOCK', this.loop_interval, */ 'COUNT', this.ID === 0 ? 0 : this.batch_size, 'STREAMS', this.name, this.ID).then(async (data) => {
                if(!semaphore) {
                    semaphore = true;
                    this.ID = '>';

                    if(this.batch_sync) {
                        await this._processTasks(data).finally(() => semaphore = false);
                    }
                    else {
                        this._processTasks(data).finally(() => semaphore = false);
                    }
                }
            });

            await sleep(this.loop_interval);
        }
    }

    async _processTasks(data) {
        if(data) {
            this.logger.info('Consumer process tasks:', this.group, data);

            const [[stream, messages]] = data;

            for (const message of messages) {
                const [id, payload] = message;

                if(!payload) {
                    await this.redis.multi()
                        .xack(stream, this.group, id)
                        .xdel(stream, id)
                        .exec();

                    continue;
                }

                let [, value, , options, , task_id, , errors] = payload || [];

                try {
                    value = JSON.parse(value);
                }
                catch(err) {
                    err
                }
                options = options ? JSON.parse(options) : {};
                errors = errors ? JSON.parse(errors) : 0;

                const worker = ({ message_id, payload, task_id }) => {
                    return this.worker.constructor.name === 'AsyncFunction' ? this.worker({ name: this.name, message_id, payload, task_id }) : new Promise((resolve, reject) => {
                        try {
                            resolve(this.worker({ name: this.name, message_id, payload, task_id }));
                        }
                        catch(err) {
                            reject(err);
                        }
                    });
                };

                let promises = [
                    !options.timeout
                        ? void 0
                        : new Promise((resolve, reject) => {
                            const timer = setTimeout(() => {
                                clearTimeout(timer);

                                reject({
                                    code: 500,
                                    message: `Task timed out after ${options.timeout} ms.`,
                                });
                            }, options.timeout);
                        }),
                    worker({ message_id: id, payload: value, task_id })
                ];

                promises = promises.filter((promise) => !!promise);

                let retries = options.retries;

                await Promise.race(promises).then(async result => {
                    if(result) {
                        const [, , , [, size]] = await this.redis.multi()
                            .xack(stream, this.group, id)
                            .xdel(stream, id)
                            .srem(`${this.name}:${DEDUPESET}`, task_id)
                            .xlen(this.name)
                            .exec();
                            
                        this.onTaskComplete && this.onTaskComplete({ name: this.name, group: this.group, consumer: this.consumer, payload: value, options, task_id, result, size });

                        this.onTaskFinal && this.onTaskFinal({ name: this.name, group: this.group, consumer: this.consumer, payload: value, options, task_id, result, size });

                        this.logger.info(`Task "${task_id}" ends with "${typeof(result) === 'object' ? JSON.stringify(result) : result}"`);
                    }
                    else {
                        if(retries < 0) {
                            retries = 2;
                            errors = 0;

                            throw { code: 400, message: 'endless queue' };
                        }

                        this.logger.info(`Task remains as pending "${task_id}"`);

                        this.onTaskPending && this.onTaskPending({ name: this.name, payload: value, options, task_id });
                    }
                }).catch(async (error) => {
                    error = prettyError(error);
                    
                    errors++;
                    
                    const retry = !!((retries === -1) || (retries && (errors < (retries || 0))));

                    if (retry) {
                        if(retries && options.delay) {
                            this.logger.info(`Delay task: ${task_id}`);

                            await sleep(options.delay);
                        }

                        const [, , [, message_id]] = await this.redis
                            .multi()
                            .xack(stream, this.group, id)
                            .xdel(stream, id)
                            //.srem(`${this.name}:${DEDUPESET}`, task_id) //DO NOT REMOVE FROM DEDUPE TO AVOID APPEND NEW TASK WITH TE SAME task_id
                            .xadd(stream, 'MAXLEN', '~', 10000, '*', 'payload', JSON.stringify(value), 'options', JSON.stringify(options), 'id', task_id, 'errors', errors)
                            .exec();

                        await this.redis.set(`${this.name}:TASK:${task_id}`, message_id);
                    } else {
                        const [, , , [, size]] = await this.redis
                            .multi()
                            .xack(stream, this.group, id)
                            .xdel(stream, id)
                            .srem(`${this.name}:${DEDUPESET}`, task_id)
                            .xlen(this.name)
                            .exec();

                        this.logger.error(`Error on task ${stream}.${task_id}`, error);

                        this.onTaskError && this.onTaskError({ name: this.name, group: this.group, consumer: this.consumer, error, payload: value, options, task_id, size });
                        
                        this.onTaskFinal && this.onTaskFinal({ name: this.name, group: this.group, consumer: this.consumer, error, payload: value, options, task_id, size });
                    }
                });
            }
        }
    }
}

module.exports = {
    RedisStreams,

    RedisStreamsQueue
};
