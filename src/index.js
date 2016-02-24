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

var state = {
    id: Math.random()
};

async.waterfall([
    init,
    main
], function (error) {
    deinit(function () {
        if (error) return logger.error(error);

        logger.info('Finish.');
    })
});

function init(callback) {
    state.client = redis.createClient(config.redis);

    state.client.on('ready', callback);
}

function deinit(callback) {
    state.client.quit();

    callback();
}

function main(callback) {
    if (cla.getErrors) {
        return flusher.run(state.client, logger, function () {
            logger.info('Flushed successfully');

            return callback();
        });
    }

    callback();
}