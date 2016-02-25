var path = require('path');

var redis = require('redis');
var minimist = require('minimist');
var async = require('async');
var logger = require('bunyan').createLogger({
    name: 'otttt'
});

var config = require(path.join(__dirname, '../cfg/config'));
var cla = minimist(process.argv.slice(2));

var Flusher = require(path.join(__dirname, './lib/flusher'));
var Writer = require(path.join(__dirname, './lib/writer'));
var Reader = require(path.join(__dirname, './lib/reader'));

const DEFAULT_ATTEMPT_INTERVAL = 4000;
const DEFAULT_MESSAGE_INTERVAL = 500;
const DEFAULT_ERRORS_LIST = 'errors';
const DEFAULT_MESSAGES_LIST = 'messages';
const DEFAULT_LOCK_KEY = 'lock';

var state = {
    id: Math.random().toString(),
    timers: {
        intervals: {},
        timeouts: {}
    },
    options: {
        attemptInterval: ( cla.attemptInterval && (cla.attemptInterval > 1000))
            ? cla.attemptInterval : DEFAULT_ATTEMPT_INTERVAL,
        messageInterval: cla.messageInterval || DEFAULT_MESSAGE_INTERVAL
    },
    storage: {
        errors: cla.errorsList || DEFAULT_ERRORS_LIST,
        messages: cla.messagesList || DEFAULT_MESSAGES_LIST,
        lock: cla.lock || DEFAULT_LOCK_KEY
    }
};

const TRY_LOCK_TTL = state.options.messageInterval + 1000;
const LOCK_TTL = state.options.messageInterval + 500;
const RESTART_DELAY = 1000;

state.options.lockTTL = LOCK_TTL;

logger.info('Starting with', state.options, state.storage);

// The main entry point.
loop();

function loop() {
    async.waterfall([
        init,
        main
    ], function (error) {
        if (error) {
            logger.error(error);
            return deinit(function () {
                setTimeout(loop, RESTART_DELAY)
            });
        }

        deinit(function () {
            logger.info('Finish.');
        });
    });
}

function init(callback) {
    state.client = redis.createClient(config.redis);

    state.client.on('ready', function () {
        logger.info('Redis client is ready.');
        return callback();
    });

    state.client.on('error', function (error) {
        logger.error(error);

        deinit(function () {
            setTimeout(loop, RESTART_DELAY)
        });
    });
}

function deinit(callback) {
    if (state.client.connected) {
        state.client.quit();
    }

    for (var interval in state.timers.intervals) {
        if (state.timers.intervals.hasOwnProperty(interval)) {
            clearInterval(state.timers.intervals[interval]);
        }
    }

    for (var timeout in state.timers.timeouts) {
        if (state.timers.timeouts.hasOwnProperty(interval)) {
            clearTimeout(state.timers.timeouts[timeout]);
        }
    }

    callback();
}

function main(callback) {
    if (cla.getErrors) {
        var flusher = new Flusher(state, logger, function (error) {
            if (error) {
                return callback(error);
            }

            logger.info('Flushed successfully');

            return callback();
        });

        return flusher.run();
    }

    var reader, writer;

    attempt();
    state.timers.intervals.attempt = setInterval(attempt, state.options.attemptInterval);

    function attempt() {

        if (reader && reader.running) {
            reader.stop();
        }

        if (writer && writer.running) {
            writer.stop();
        }

        state.client.set(state.storage.lock, state.id, 'NX', 'PX', TRY_LOCK_TTL,
            function (error, result) {
                if (error) {
                    return callback(error);
                }

                if (result) {
                    // lock is successfully set, I'm the writer
                    logger.info('New writer mode.');
                    writer = new Writer(state, logger, callback);
                    return writer.run();
                }

                state.client.get(state.storage.lock, function (error, value) {
                    if (error) {
                        return callback(error);
                    }

                    if (value == state.id) {
                        // Lock value is my state.id, I'm the writer
                        logger.info('Old writer mode.');
                        writer = new Writer(state, logger, callback);
                        writer.run();
                    } else {
                        // Lock is set and it's value is not my state.is,
                        // so I'm a reader
                        logger.info('Reader mode.');
                        reader = new Reader(state, logger, callback);
                        reader.run();
                    }
                });


            });
    }
}