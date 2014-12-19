/**
 * Created by daniel.joppi on 12/18/14.
 */
var _ = require('lodash'),
    zlib = require('zlib'),
    async = require('async'),
    argv = require('optimist').argv,
    debug = argv['dyn-debug'],
    Stream = require('stream').Stream,
    AWS = require('aws-sdk');

var _parser = require('../parser'),
    _refiner = require('../refiner'),
    _index = require('../indexer'),
    _modify = require('../capacity');

const _collect = require('./st/collect'),
    _error = require('../utils/error'),
    _item = require('../utils/item');
module.exports = function(dyn, finder, parser, backup, db, _alias) {
    return function (table) {
        table.ensuredIndexes = [];

        table.find = function (cond, projection, identity) {
            var p, modifiers = {}, table = this;

            modifiers.$consistent = !!table.$consistent;

            p = dyn.promise(['results', 'count', 'end'], null, 'consumed');

            process.nextTick(function () {
                parser
                    .parse(table, modifiers, cond, projection, identity)
                    .parsed(function (query) {
                        refiner = _refiner(dyn, query, db),
                            cursor = finder.find(query);
                        cursor.chain(refiner);
                        refiner.chain(p);
                    })
                    .error(p.trigger.error);
            });

            p.sort = function (o) {
                modifiers.orderby = o;
                return p;
            };

            p.limit = function (n) {
                modifiers.limit = n;
                return p;
            };

            p.window = function (n) {
                modifiers.window = n;
                return p;
            };

            p.skip = function (n) {
                modifiers.skip = n;
                return p;
            };

            p.noderef = function () {
                modifiers.noderef = true;
                return p;
            };

            var origCount = _.bind(p.count, p);

            p.count = function (fn) {
                if (fn)
                    origCount(fn);
                else
                    modifiers.count = true;

                return p;
            };

            return p;
        };

        table.findOne = function () {
            var p, args = arguments, table = this;

            p = dyn.promise('result', 'notfound', 'consumed');

            table.find.apply(table, args).limit(1).results(function (items) {
                if (items.length == 0)
                    p.trigger.notfound();
                else
                    p.trigger.result(items[0]);
            })
                .consumed(p.trigger.consumed)
                .error(p.trigger.error);

            return p;
        };

        table.consistent = function () {
            return _.extend({$consistent: true}, table);
        };

        table.save = function (_obj, isCreate) {
            var objs = _obj ? (Array.isArray(_obj) ? _obj : [_obj]) : [],
                consume = {},
                p = dyn.promise(null, 'updatedsinceread', 'consumed'), found = false;

            process.nextTick(function () {

                async.forEach(objs,
                    function (obj, done) {
                        var gops = {},
                            ops = gops[table._dynamo.TableName] = [],
                            _save = require('./st/save')(ops, gops),
                            _mput = require('./st/mput')(dyn, isCreate, consume);


                        _save(obj, isCreate, true);

                        _.keys(gops).forEach(function (table) {
                            if (gops[table].length == 0)
                                delete gops[table];
                            else
                                found = true;
                        });

                        if (found)
                            _mput(gops, done);
                        else
                            process.nextTick(done);

                    },
                    function (err) {
                        p.trigger.consumed(consume);

                        if (err) {
                            if (err.code == 'notfound')
                                p.trigger.updatedsinceread();
                            else
                                p.trigger.error(err);
                        }
                        else
                            p.trigger.success();
                    });

            });

            return p;
        };

        table.create = function (obj) {
            var p = dyn.promise(null, 'exists', 'consumed');

            table.save(obj, true)
                .success(p.trigger.success)
                .consumed(p.trigger.consumed)
                .error(function (err) {
                    if (err.code == 'found')
                        p.trigger.exists();
                    else
                        p.trigger.error(err);
                });

            return p;
        };

        table.enableIndex = function (fields) {
            var index = _index(dyn, table, fields, opts);
            table.indexes.push(index);
            table.ensuredIndexes.push(fields);
        };

        table.ensureIndex = function (fields) {
            var p = dyn.promise();

            process.nextTick(function () {
                var index = _index(dyn, table, fields, opts);

                if (index)
                    index.ensure(function (err) {
                        if (err)
                            p.trigger.error(err);
                        else {
                            table.indexes.push(index);
                            table.ensuredIndexes.push(fields);
                            p.trigger.success();
                        }
                    });
                else
                    p.trigger.error(new Error('no known index type can index those fields'));
            });

            return p;
        };

        table.remove = function (filter) {
            var p = dyn.promise(null, null, 'consumed'),
                consume = {},
                _consumed = function (cons) {
                    consume.read += cons.read;
                    consume.write += cons.write;
                },
                _error = function (err) {
                    p.trigger.consumed(consume);
                    p.trigger.error(err);
                },
                _success = function () {
                    p.trigger.consumed(consume);
                    p.trigger.success();
                },
                sync = dyn.syncResults(function (err) {
                    if (err)
                        _error(err);
                    else
                        _success();
                }),
                cursor = table.find(filter, table.indexes.length ? undefined : {_id: 1, _pos: 1}),
                _deleteItem = function (obj, done) {
                    async.parallel([
                            function (done) {
                                async.forEach(table.indexes,
                                    function (index, done) {
                                        index.remove(obj).success(done).error(done).consumed(_collect(consume));
                                    }, done);
                            },
                            function (done) {
                                dyn.table(table._dynamo.TableName)
                                    .hash('_id', obj._id)
                                    .range('_pos', obj._pos)
                                    .delete(done)
                                    .consumed(_collect(consume))
                                    .error(done);
                            }],
                        done);
                };

            if (table.indexes.length == 0)
                cursor = cursor.noderef();

            cursor.results(sync.results(function (items, done) {
                async.forEach(items, _deleteItem, done);
            }))
                .consumed(_consumed)
                .error(_error)
                .end(sync.end);

            return p;
        };

        table.update = function (query, update) {
            var p = dyn.promise(null, null, 'consumed'),
                cursor = table.consistent().find(query),
                consume = {},
                _consumed = function (cons) {
                    _.keys(cons).forEach(function (table) {
                        var c, tcons = cons[table];

                        if (!(c = consume[table]))
                            c = consume[table] = {read: 0, write: 0};

                        c.read += tcons.read;
                        c.write += tcons.write;
                    });
                },
                _error = function (err) {
                    p.trigger.consumed(consume);
                    p.trigger.error(err);
                },
                _success = function () {
                    p.trigger.consumed(consume);
                    p.trigger.success();
                },
                sync = dyn.syncResults(function (err) {
                    if (err)
                        _error(err);
                    else
                        _success();
                }),
                _updateItem = function (item, done) {
                    if (update.$set)
                        table.save(_.extend(item, update.$set))
                            .success(done)
                            .consumed(_consumed)
                            .error(done);
                    else if (update.$unset)
                        table.save(_.omit(item, _.keys(update.$unset)))
                            .success(done)
                            .consumed(_consumed)
                            .error(done);
                    else if (update.$inc) {
                        var fields = {};

                        _.keys(update.$inc).forEach(function (name) {
                            fields[name] = {action: 'ADD', value: update.$inc[name]};
                        });

                        dyn.table(item._table)
                            .hash('_id', item._id)
                            .range('_pos', item._pos)
                            .updateItem({update: fields}, function () {
                                done();
                            })
                            .consumed(_collect(consume))
                            .error(done);
                    }
                    else
                        done(new Error('unknown update type'));
                },
                _updateItems = function (items, done) {
                    async.forEach(items, _updateItem, done);
                };


            cursor
                .results(sync.results(_updateItems))
                .consumed(_consumed)
                .error(_error)
                .end(sync.end);

            return p;
        };

        table.modify = function (read, write) {
            return _modify(dyn, table._dynamo.TableName, read, write);
        };

        table.drop = function () {
            var p = dyn.promise(),
                _success = function () {
                    delete db[_alias(table._dynamo.TableName)];
                    p.trigger.success();
                },
                _check = function () {
                    dyn.describeTable(table._dynamo.TableName,
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

            async.forEach(table.indexes,
                function (index, done) {
                    index.drop(done);
                },
                function (err) {
                    if (opts.hints) console.log('This may take a while...'.yellow);

                    dyn.deleteTable(table._dynamo.TableName, function (err) {
                        if (err) {
                            if (err.code == 'ResourceNotFoundException')
                                _success();
                            else
                                p.trigger.error(err);
                        }
                        else
                            setTimeout(_check, 5000);
                    });
                });

            return p;
        };

        //table.backup = backup.backup(table._dynamo.TableName);

        //table.restore = backup.restore(table._dynamo.TableName);

        return table;
    };
};