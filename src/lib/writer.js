var getMessage = require('./vendor').getMessage;

module.exports = function Writer(state, logger, callback) {

    this.running = false;

    var writer = this;

    this.run = function () {
        writer.running = true;

        step();

        function step() {
            var message = getMessage();
            state.client
                .multi()
                .set(state.storage.lock, state.id, 'XX', 'PX', state.options.lockTTL,
                    function (error) {
                        if (error) {
                            writer.stop();
                            return callback(error);
                        }
                    })
                .rpush('messages', message, function (error, value) {
                    if (error) {
                        writer.stop();
                        return callback(error);
                    }
                    if (!error) {
                        logger.debug('pushed message, size', value);
                        if (!(message % 500)) {
                            logger.info('pushed message', message, 'size', value);
                        }
                    }
                })
                .exec(function (error) {
                    if (error) {
                        writer.stop();
                        return callback(error);
                    }

                    if (writer.running) {
                        state.timers.timeouts.write = setTimeout(step,
                            state.options.messageInterval);
                    }
                });
        }
    };

    this.stop = function () {
        writer.running = false;
    }
};