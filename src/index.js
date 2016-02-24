var path = require('path');

var redis = require('redis');
var minimist = require('minimist');
var async = require('async');
var logger = require('bunyan').createLogger({
    name: 'otttt',
    level: 0
});

var config = require(path.join(__dirname, '../cfg/config.js'));
var cla = minimist(process.argv.slice(2));

var flusher = require(path.join(__dirname, './lib/flusher.js'));
var writer = require(path.join(__dirname, './lib/writer.js'));

var state = {
    id: Math.random()
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

    if (state.times && state.times.intervals) {
        for (var interval in state.times.intervals) {
            if (state.times.intervals.hasOwnProperty(interval)) {
                clearInterval(state.times.intervals[interval]);
            }
        }
    }

    callback();
}

function main(callback) {
    if (cla.getErrors) {
        return flusher.run(state.client, logger, function (error) {
            if (error) return callback(error);

            logger.info('Flushed successfully');

            return callback();
        });
    }

    state.client.set('writer', state.id, 'NX', 'PX', 1000, function (error, result) {
        if (error) {
            return callback(error);
        }

        if (result) {
            // I'm the writer
            logger.trace('Writer mode.');
            return writer.run(state, logger, callback);
        } else {
            // I'm a reader
            logger.trace('Reader mode.');
            return callback();
        }
    });
}