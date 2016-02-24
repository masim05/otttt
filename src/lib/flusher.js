var async = require('async');

module.exports.run = function (client, logger, callback) {

    step();

    function step() {
        // TODO move list name to the config
        client.lpop('errors', function (error, value) {
            if (error) return callback(error);

            if (!value) return callback();

            logger.info('error', value);

            step();
        });
    };
};