/**
 * Created by daniel.joppi on 12/19/14.
 */
var _ = require('lodash'),
    async = require('async'),
    cclone = require('circularclone');

const _collect = require('./collect'),
    _nu = require('../../utils/nu');

module.exports = function(dyn, isCreate, consume) {

    return function (gops, done) {
        async.forEach(_.keys(gops),
            function (_table, done) {
                var tops = gops[_table];

                async.forEachSeries(tops, // forEachSeries: when deleting elements from array i need deletes of old item _pos done before new item _pos put
                    function (op, done) {
                        var tab = dyn.table(_table),
                            obj = op.item;

                        if (op.index)
                            tab.hash('_hash', obj._hash)
                                .range('_range', obj._range);
                        else
                            tab.hash('_id', obj._id)
                                .range('_pos', obj._pos);

                        if (op.op == 'put')
                            tab.put(_.omit(obj, op.omit),
                                function () {
                                    obj._old = cclone(_.omit(obj, '_old'));
                                    done();
                                },
                                {
                                    expected: obj._old && _nu(obj._old._rev) ? {_rev: obj._old._rev} : undefined,
                                    exists: isCreate ? false : undefined
                                })
                                .consumed(_collect(consume))
                                .error(done);
                        else if (op.op == 'del')
                            tab.delete(done)
                                .error(done);
                        else
                            done(new Error('unknown update type:' + op.op));
                    },
                    done);
            },
            done);
    };
};