var getMessage = require('./vendor').getMessage;

module.exports = function Writer(state, logger, callback) {

    this.run = function () {

        state.timers.intervals.lock = setInterval(function () {
            var message = getMessage();
            state.client
                .multi()
                .set(state.storage.lock, state.id, 'XX', 'PX', state.options.lockTTL, function (error) {
                    if (error) callback(error);
                })
                .rpush('messages', message, function (error, value) {
                    if (error) callback(error);
                    if (!error) {
                        logger.debug('pushed message, size', value);
                        if (!(message % 500)) {
                            logger.info('pushed message', message, 'size', value);
                        }
                    }
                })
                .exec();
        }, state.options.messageInterval);
    };
};