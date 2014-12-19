/**
 * Created by daniel.joppi on 12/19/14.
 */
var _ = require('lodash');

module.exports = function (success, error, info) {
    success = success || [];
    error = error || [];
    info = info || [];

    success = Array.isArray(success) ? success : [success];
    error = Array.isArray(error) ? error : [error];
    info = Array.isArray(info) ? info : [info];

    var promise = {queue: {}, trigger: {}},
        _conf = function (also) {
            return function (arg) {
                promise.queue[arg] = [];

                promise[arg] = function (cb) {
                    if (!(cb instanceof Function))
                        throw new Error('Trying to bind a promise without a callback: ' + cb);

                    promise.queue[arg].push(cb);
                    return promise;
                };

                promise.trigger[arg] = function () {
                    var args = arguments;

                    promise.queue[arg].forEach(function (cb) {
                        cb.apply(null, args);
                    });

                    also && also(arg);
                };
            };
        };

    _.union(['success', 'error'], info).forEach(_conf());

    success.forEach(_conf(promise.trigger.success));
    error.forEach(_conf(function (code) {
        promise.trigger.error(_error({code: code}), true);
    }));

    promise.trigger.error = function (err, also) {
        if (!also && promise.trigger[err.code])
            promise.trigger[err.code]();
        else
            promise.queue.error.forEach(function (cb) {
                cb.apply(null, [err]);
            });
    };

    promise.should = function (what) {
        return function (err) {
            if (err)
                promise.trigger.error(err);
            else {
                var args = Array.prototype.slice.apply(arguments);
                args.shift();

                promise.trigger[what].apply(args);
            }
        };
    };

    promise.chain = function (p) {
        _.union(success, info).forEach(function (type) {
            var trg = p.trigger[type];
            if (trg) promise[type](p.trigger[type]);
        });
        promise.error(p.trigger.error);
    };

    return promise;
};