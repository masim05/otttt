var async = require('async');

module.exports = function Writer(state, logger, callback) {

    this.run = function () {

        step();

        function step() {
            state.client.lpop(state.storage.errors, function (error, value) {
                if (error) return callback(error);

                if (!value) return callback();

                logger.info('error', value);

                step();
            });
        }
    }
};