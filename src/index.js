var path = require('path');

var redis = require('redis');
var minimist = require('minimist');
var async = require('async');
var logger = require('bunyan').createLogger({
    name: 'otttt',
    level: 0
});

var config = require(path.join(__dirname, '../cfg/config'));
var cla = minimist(process.argv.slice(2));

var Flusher = require(path.join(__dirname, './lib/flusher'));
var Writer = require(path.join(__dirname, './lib/writer'));
var Reader = require(path.join(__dirname, './lib/reader'));

const ATTEMPT_INTERVAL = 4000;

var state = {
    id: Math.random(),
    timers: {
        intervals: {},
        timeouts: {}
    }
};

function loop() {
    async.waterfall([
        init,
        main
    ], function (error) {
        if (error) {
            logger.error(error);
            return deinit(loop);
        }

        deinit(function () {
            logger.info('Finish.');
        });
    });
}

loop();

function init(callback) {
    state.client = redis.createClient(config.redis);

    state.client.on('ready', function () {
        logger.trace('Redis client is ready.');
        return callback();
    });

    state.client.on('error', function (error) {
        logger.error(error);

        deinit(loop);
    });
}

function deinit(callback) {
    state.client.quit();

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

    var reader;

    attempt();
    state.timers.intervals.attempt = setInterval(attempt, ATTEMPT_INTERVAL);

    function attempt() {

        if (reader && reader.running) {
            reader.stop();
        }

        state.client.set('writer', state.id, 'NX', 'PX', 1000, function (error, result) {
            if (error) {
                return callback(error);
            }

            if (result) {
                // I'm the writer
                logger.trace('Writer mode.');
                clearInterval(state.timers.intervals.attempt);
                var writer = new Writer(state, logger, callback);
                return writer.run();
            } else {
                // I'm a reader
                logger.trace('Reader mode.');
                reader = new Reader(state, logger, callback);
                reader.run();
            }
        });
    }
}