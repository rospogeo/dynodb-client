var dyno = require('./lib/dyn.js'),
    diff = require('deep-diff').diff,
    uuid = require('node-uuid').v4,
    _ = require('lodash'),
    cclone = require('circularclone'),
    async = require('async');

var defer = require("promised-io/promise").Deferred;

var _parser = require('./lib/parser'),
    _finder = require('./lib/finder'),
    _refiner = require('./lib/refiner'),
    _index = require('./lib/indexer'),
    _modify = require('./lib/capacity'),
    _backup = require('./lib/backup');

const _nu = require('./lib/utils/nu'),
    _isobject = require('./lib/utils/isobject'),
    _isobjectarr = require('./lib/utils/isarray'),
    _collect =require('./lib/db/st/collect');

var dyngo = module.exports = function (opts, cb) {
    var defaults = {hints: true};

    if (!cb) {
        cb = opts;
        opts = defaults;
    }

    opts = opts || defaults;
    opts = _.defaults(opts, defaults);

    var dyn = dyno(opts.dynamo, _.extend(opts.tx || {}, {txTable: opts.txTable})),
        finder = _finder(dyn),
        parser = _parser(dyn, opts),
        backup = _backup(dyn, opts),
        db = _.extend({_dyn: dyn}, opts.tx, {txTable: opts.txTable}),
        _alias = function (table) {
            return (opts.tables || {} )[table] || table;
        };

    db.cleanup = function (obj) {
        var p = dyn.promise('clean');

        process.nextTick(function () {
            p.trigger.clean(cclone(obj, function (key, value) {
                if (key.indexOf && key.indexOf('_') == 0 && key != '_id')
                    return undefined;
                else
                    return value;
            }));
        });

        return p;
    };


    var configureTable = require('./lib/db/table')(dyn, finder, parser, backup, db, _alias),
        configureTables = function (cb) {
            var configure = function (tables) {
                async.forEach(Object.keys(tables),
                    function (table, done) {
                        dyn.describeTable(table, function (err, data) {
                            if (!err) {
                                var hash = _.findWhere(data.Table.KeySchema, {KeyType: 'HASH'}),
                                    range = _.findWhere(data.Table.KeySchema, {KeyType: 'RANGE'});

                                if (hash && hash.AttributeName && hash.AttributeName == '_id' && range && range.AttributeName == '_pos')
                                    db[tables[table]] = configureTable({_dynamo: data.Table, indexes: []});
                            }

                            done(err);
                        });
                    },
                    function (err) {
                        cb(err, err ? null : db);
                    });
            };

            if (opts.tables)
                configure(opts.tables);
            else
                dyn.listTables(function (err, list) {
                    if (err)
                        cb(err);
                    else {
                        var tables = {};
                        list.forEach(function (table) {
                            tables[table] = table;
                        });
                        configure(tables);
                    }
                });
        };

    db.createCollection = function (name, p) {
        var deferred = defer(),
            p = p || dyn.promise(),
            _success = function () {
                dyn.describeTable(name, function (err, data) {
                    if (!err) {
                        db[name] = configureTable({_dynamo: data.Table, indexes: []});
                        p.trigger.success();
                        console.log('caraio de tabela '+name);
                        deferred.resolve(db[name]);
                    }
                    else {
                        p.trigger.error(err);
                        console.log('erro', err);
                    }
                });
            };

        if (opts.hints) console.log('This may take a while...'.yellow);

        dyn.table(name)
            .hash('_id', 'S')
            .range('_pos', 'N')
            .create(function check() {
                dyn.table(name)
                    .hash('_id', 'xx')
                    .query(function () {
                        _success();
                    })
                    .error(function (err) {
                        if (err.code == 'ResourceNotFoundException')
                            setTimeout(check, 5000);
                        else if (err.code == 'notfound')
                            _success();
                        else
                            p.trigger.error(err);
                    });
            })
            .error(function (err) {
                if (err.code == 'ResourceInUseException')
                    p.trigger.error(new Error('the collection exists'));
                else
                    p.trigger.error(err);
            });
console.log('fdp');
        var d = deferred.promise;
        console.log('caraio '+name);
        return d;
    };

    db.collection = function (name) {
        if (db[name])
            return db[name];
        else
            return db.createCollection(name);
    };

    db.ensureTransactionTable = function (topts) {
        topts = _.defaults(topts || {}, {name: 'dyngo-transaction-table'});

        var p = dyn.promise(),
            _success = function () {
                dyn.describeTable(topts.name, function (err, data) {
                    if (!err) {
                        db.txTable = {_dynamo: data.Table, indexes: []};

                        db.txTable.modify = function (read, write) {
                            return _modify(dyn, data.Table.TableName, read, write)
                        };

                        db.txTable.drop = function () {
                            var p = dyn.promise(),
                                _success = function () {
                                    delete db.txTable;
                                    p.trigger.success();
                                },
                                _check = function () {
                                    dyn.describeTable(data.Table.TableName,
                                        function (err, data) {
                                            if (err) {
                                                if (err.code == 'ResourceNotFoundException')
                                                    _success();
                                                else
                                                    p.trigger.error(err);
                                            }
                                            else
                                                setTimeout(_check, 5000);
                                        });
                                };

                            if (opts.hints) console.log('This may take a while...'.yellow);

                            dyn.deleteTable(data.Table.TableName, function (err) {
                                if (err) {
                                    if (err.code == 'ResourceNotFoundException')
                                        _success();
                                    else
                                        p.trigger.error(err);
                                }
                                else
                                    setTimeout(_check, 5000);
                            });

                            return p;
                        };

                        p.trigger.success();
                    }
                    else
                        p.trigger.error(err);
                });
            };

        if (opts.hints) console.log('This may take a while...'.yellow);

        dyn.table(topts.name)
            .hash('_id', 'S')
            .range('_item', 'S')
            .create(function check() {
                dyn.table(topts.name)
                    .hash('_id', 'xx')
                    .query(function () {
                        _success();
                    })
                    .error(function (err) {
                        if (err.code == 'ResourceNotFoundException')
                            setTimeout(check, 5000);
                        else if (err.code == 'notfound')
                            _success();
                        else
                            p.trigger.error(err);
                    });
            })
            .error(function (err) {
                if (err.code == 'ResourceInUseException')
                    _success();
                else
                    p.trigger.error(err);
            });

        return p;
    };

    db.transaction = function (txOpts) {
        var p = dyn.promise('transaction', null, 'consumed'),
            consume = {};

        process.nextTick(function () {
            if (!db.txTable) {
                p.trigger.error(new Error('no transaction table defined'));
                return;
            }

            var tab = dyn.table(db.txTable._dynamo.TableName),
                init = function (tx) {
                    dyn.table(db.txTable._dynamo.TableName)
                        .hash('_id', tx._id)
                        .range('_item', '_')
                        .put(tx, function () {
                            var dopts = _.extend({tx: tx, txTable: db.txTable}, opts, txOpts);

                            dyngo(dopts,
                                function (err, tx) {
                                    if (err) {
                                        p.trigger.error(err);
                                        return;
                                    }

                                    _.filter(_.keys(tx), function (key) {
                                        return !!tx[key].find;
                                    })
                                        .forEach(function (tableName) {
                                            db[tableName].ensuredIndexes.forEach(tx[tableName].enableIndex); // use enableIndex (sync) do not ensure..
                                        });

                                    dopts.tx.transaction = _.bind(db.transaction, db);

                                    tx.commit = function () {
                                        var p = dyn.promise('committed', 'rolledback', 'consumed'),
                                            consume = {},
                                            _commit = function (cb) {
                                                dyn.table(db.txTable._dynamo.TableName)
                                                    .hash('_id', tx._id)
                                                    .range('_item', '_')
                                                    .updateItem({
                                                        update: {state: {action: 'PUT', value: 'committed'}},
                                                        expected: {state: 'pending'}
                                                    },
                                                    function () {
                                                        tx.state = 'committed';
                                                        cb();
                                                    })
                                                    .consumed(_collect(consume))
                                                    .error(function (err) {
                                                        if (err.code == 'notfound')
                                                            p.trigger.rolledback(true);
                                                        else
                                                            p.trigger.error(err);
                                                    });
                                            },
                                            _complete = function (cb) {
                                                var sync = dyn.syncResults(function (err) {
                                                    if (err)
                                                        p.trigger.error(err);
                                                    else
                                                        cb();
                                                });

                                                dyn.table(db.txTable._dynamo.TableName)
                                                    .hash('_id', tx._id)
                                                    .range('_item', 'target::', 'BEGINS_WITH')
                                                    .query(sync.results(function (items, done) {
                                                        async.forEach(items,
                                                            function (item, done) {
                                                                var _item = item._item.split('::'),
                                                                    table = _item[1],
                                                                    hash = {attr: _item[2], value: _item[3]},
                                                                    range = {
                                                                        attr: _item[4],
                                                                        value: _item[4] == '_pos' ? +_item[5] : _item[5]
                                                                    };

                                                                if (item._txOp == 'delete')
                                                                    dyn.table(table)
                                                                        .hash(hash.attr, hash.value)
                                                                        .range(range.attr, range.value)
                                                                        .delete(function () {
                                                                            done();
                                                                        }, {expected: {_tx: tx._id}})
                                                                        .consumed(_collect(consume))
                                                                        .error(done);
                                                                else
                                                                    dyn.table(table)
                                                                        .hash(hash.attr, hash.value)
                                                                        .range(range.attr, range.value)
                                                                        .updateItem({
                                                                            update: {
                                                                                _txTransient: {action: 'DELETE'},
                                                                                _txApplied: {action: 'DELETE'},
                                                                                _txDeleted: {action: 'DELETE'},
                                                                                _txLocked: {action: 'DELETE'},
                                                                                _tx: {action: 'DELETE'}
                                                                            }
                                                                        },
                                                                        function () {
                                                                            done();
                                                                        })
                                                                        .consumed(_collect(consume))
                                                                        .error(done);

                                                            },
                                                            done);
                                                    }),
                                                    {
                                                        attrs: ['_id', '_item', '_txOp'],
                                                        consistent: true
                                                    })
                                                    .error(p.trigger.error)
                                                    .consumed(_collect(consume))
                                                    .end(sync.end);
                                            },
                                            _clean = function (cb) {
                                                dyn.table(db.txTable._dynamo.TableName)
                                                    .hash('_id', tx._id)
                                                    .range('_item', '_')
                                                    .updateItem({
                                                        update: {state: {action: 'PUT', value: 'completed'}},
                                                        expected: {state: 'committed'}
                                                    },
                                                    function () {
                                                        tx.state = 'completed';
                                                        cb();
                                                    })
                                                    .consumed(p.trigger.consumed)
                                                    .error(p.trigger.error);
                                            },
                                            _committed = function () {
                                                p.trigger.consumed(consume);
                                                p.trigger.committed();
                                            };

                                        if (tx.state == 'pending')
                                            _commit(function () {
                                                _complete(function () {
                                                    _clean(_committed);
                                                });
                                            });
                                        else if (tx.state == 'committed')
                                            _complete(function () {
                                                _clean(_committed);
                                            });
                                        else
                                            p.trigger.error(new Error("Invalid transaction state: " + tx.state));

                                        return p;
                                    };

                                    tx.rollback = function () {
                                        var p = dyn.promise('rolledback', null, 'consumed'),
                                            consume = {},
                                            _rollback = function (cb) {
                                                var sync = dyn.syncResults(function (err) {
                                                    if (err)
                                                        p.trigger.error(err);
                                                    else
                                                        dyn.table(db.txTable._dynamo.TableName)
                                                            .hash('_id', tx._id)
                                                            .range('_item', '_')
                                                            .updateItem({
                                                                update: {state: {action: 'PUT', value: 'rolledback'}},
                                                                expected: {state: 'pending'}
                                                            },
                                                            function () {
                                                                tx.state = 'rolledback';
                                                                cb();
                                                            })
                                                            .consumed(_collect(consume))
                                                            .error(p.trigger.error);
                                                });

                                                dyn.table(db.txTable._dynamo.TableName)
                                                    .hash('_id', tx._id)
                                                    .range('_item', 'target::', 'BEGINS_WITH')
                                                    .query(sync.results(function (items, done) {
                                                        async.forEach(items,
                                                            function (item, done) {
                                                                var _item = item._item.split('::'),
                                                                    table = _item[1],
                                                                    hash = {attr: _item[2], value: _item[3]},
                                                                    range = {
                                                                        attr: _item[4],
                                                                        value: _item[4] == '_pos' ? +_item[5] : _item[5]
                                                                    },
                                                                    clean = function () {
                                                                        dyn.table(table)
                                                                            .hash(hash.attr, hash.value)
                                                                            .range(range.attr, range.value)
                                                                            .updateItem({
                                                                                update: {
                                                                                    _txTransient: {action: 'DELETE'},
                                                                                    _txApplied: {action: 'DELETE'},
                                                                                    _txDeleted: {action: 'DELETE'},
                                                                                    _txLocked: {action: 'DELETE'},
                                                                                    _tx: {action: 'DELETE'}
                                                                                }
                                                                            },
                                                                            function () {
                                                                                done();
                                                                            })
                                                                            .consumed(_collect(consume))
                                                                            .error(done);
                                                                    };

                                                                if (_.contains(['put', 'updateItem'], item._txOp))
                                                                    dyn.table(table)
                                                                        .hash(hash.attr, hash.value)
                                                                        .range(range.attr, range.value)
                                                                        .get(function (item) {
                                                                            if (item._txTransient)
                                                                                dyn.table(table)
                                                                                    .hash(hash.attr, hash.value)
                                                                                    .range(range.attr, range.value)
                                                                                    .delete(function () {
                                                                                        done();
                                                                                    })
                                                                                    .consumed(_collect(consume))
                                                                                    .error(done);
                                                                            else
                                                                                dyn.table(db.txTable._dynamo.TableName)
                                                                                    .hash('_id', tx._id)
                                                                                    .range('_item', ['copy',
                                                                                        table,
                                                                                        hash.attr,
                                                                                        hash.value,
                                                                                        range.attr,
                                                                                        range.value].join('::'))
                                                                                    .get(function (copy) {
                                                                                        copy = _.omit(copy, ['_id',
                                                                                            '_item',
                                                                                            '_txLocked',
                                                                                            '_txApplied',
                                                                                            '_txDeleted',
                                                                                            '_txTransient',
                                                                                            '_tx']);

                                                                                        copy[hash.attr] = hash.value;
                                                                                        copy[range.attr] = range.value;

                                                                                        dyn.table(table)
                                                                                            .hash(hash.attr, hash.value)
                                                                                            .range(range.attr, range.value)
                                                                                            .put(copy, function () {
                                                                                                done();
                                                                                            })
                                                                                            .consumed(p.trigger.consumed)
                                                                                            .error(p.trigger.error);
                                                                                    })
                                                                                    .consumed(_collect(consume))
                                                                                    .error(done);
                                                                        })
                                                                        .consumed(_collect(consume))
                                                                        .error(done);
                                                                else
                                                                    clean();
                                                            },
                                                            done);
                                                    }),
                                                    {
                                                        attrs: ['_id', '_item', '_txOp'],
                                                        consistent: true
                                                    })
                                                    .error(p.trigger.error)
                                                    .consumed(_collect(consume))
                                                    .end(sync.end);
                                            };

                                        if (tx.state == 'pending')
                                            _rollback(p.trigger.rolledback);
                                        else
                                            p.trigger.error(new Error("Invalid transaction state: " + tx.state));

                                        return p;
                                    };

                                    p.trigger.consumed(consume);
                                    p.trigger.transaction(tx);
                                });
                        })
                        .consumed(_collect(consume))
                        .error(p.trigger.error);

                };

            if (typeof txOpts == 'string')
                tab.hash('_id', txOpts)
                    .range('_item', '_')
                    .get(init, {consistent: true})
                    .consumed(_collect(consume))
                    .error(p.trigger.error);
            else {
                if (opts.tx)
                    p.trigger.error(new Error('cannot start a transaction within a transaction'));
                else
                    init({_id: uuid(), _item: '_', state: 'pending'});
            }
        });

        return p;
    };

    configureTables(cb);

    return db;
};
