var async = require('async');

module.exports = function Writer(state, logger, callback) {

    this.run = function () {

        step();

        function step() {
            // TODO move list name to the config
            state.client.lpop('errors', function (error, value) {
                if (error) return callback(error);

                if (!value) return callback();

                logger.info('error', value);

                step();
            });
        }
    }
};