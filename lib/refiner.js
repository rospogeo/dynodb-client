var _ = require('lodash');

const  _collect = require('./db/refine/collect'),
    _refineItems = require('./db/refine/refine-items');

module.exports = function (dyn, query, db) {
    var promises = ['results', 'end'];

    if (query.count) promises.push('count');

    var refiner = dyn.promise(promises, null, 'consumed'), fended, ended, _end = refiner.trigger.end, _results, _items, consume = {}, _consumed = refiner.trigger.consumed,
        _stop = function () {
            ended = true;
        };

    refiner.trigger.end = _.wrap(refiner.trigger.end, function (trigger) {
        fended = true;
    });

    refiner.trigger.consumed = _.wrap(refiner.trigger.consumed, function (trigger, cons) {
        var c;

        if (!(c = consume[cons.table]))
            c = consume[cons.table] = {read: 0, write: 0};

        c.read += cons.read;
        c.write += cons.write;
    });

    refiner.trigger.results = _.wrap(refiner.trigger.results, function (trigger, items) {
        _results = trigger;

        if (!ended)
            process.nextTick(function () // so we can call _end
            {
                _refineItems(dyn, function (items) {
                    if (items.next && !query.limited)
                        items.next();

                    ended = query.limited || fended;

                    delete items.next;

                    query.$returned += items.length;

                    if (query.count) {
                        process.stdout.write(('\r' + query.$returned).yellow);

                        if (query.canCount()) {
                            _consumed(consume);
                            refiner.trigger.count(query.$returned);
                        }
                    }
                    else if (items.length > 0)
                        _results(items, _stop);

                    if (ended) {
                        if (query.$returned == 0)
                            _results([]);

                        _consumed(consume);
                        _end();
                    }
                },
                items, query, db).consumed(_collect(consume));
            });
    });

    return refiner;
};
