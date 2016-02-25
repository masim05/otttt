var getMessage = require('./vendor').getMessage;

module.exports = function Writer(state, logger, callback) {

    this.run = function () {

        state.timers.intervals.lock = setInterval(function () {
            state.client
                .multi()
                .set(state.storage.lock, state.id, 'XX', 'PX', state.options.lockTTL, function (error) {
                    if (error) callback(error);
                })
                .rpush('messages', getMessage(), function (error, value) {
                    if (error) callback(error);
                    if (!error) logger.debug('pushed message, size', value);
                })
                .exec();
        }, state.options.messageInterval);
    };
};