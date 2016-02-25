var eventHandler = require('./vendor').eventHandler;
const EMPTY_LIST_DELAY = 100;

module.exports = function Reader(state, logger, callback) {

    this.running = false;

    var reader = this;

    this.run = function () {
        reader.running = true;

        step();

        function step() {
            state.client.lpop('messages', function (error, value) {
                if (error) {
                    reader.stop();
                    return callback(error);
                }


                if (!value)
                    return state.timers.timeouts.reader = setTimeout(step, EMPTY_LIST_DELAY);

                logger.trace('got message', value);

                eventHandler(value, function (error, value) {
                    logger.info('processed message', {error: error, value: value});

                    state.client.rpush('errors', value, function (error) {
                        if (error) {
                            reader.stop();
                            return callback(error);
                        }

                        if (reader.running)
                            return step();
                    });
                });
            });
        }
    };

    this.stop = function () {
        reader.running = false;
    };
};