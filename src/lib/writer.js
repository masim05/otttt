var getMessage = require('./vendor').getMessage;
const MESSAGE_INTERVAL = 500;
const LOCK_EXPIRATION = 1000;

module.exports.run = function (state, logger, callback) {

    state.timers.intervals.lock = setInterval(function () {
        state.client
            .multi()
            .set('writer', state.id, 'EX', 'PX', LOCK_EXPIRATION, function (error) {
                if (error) callback(error);
            })
            .rpush('messages', getMessage(), function (error) {
                if (error) callback(error);
            })
            .exec();
    }, MESSAGE_INTERVAL);
};