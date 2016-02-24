module.exports.eventHandler = function eventHandler(msg, callback) {
    function onComplete() {
        var error = Math.random() > 0.85;
        callback(error, msg);
    }

    setTimeout(onComplete, Math.floor(Math.random() * 1000));
};

module.exports.getMessage = function getMessage() {
    this.cnt = this.cnt || 0;
    return this.cnt++;
};