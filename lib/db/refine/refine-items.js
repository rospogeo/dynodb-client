/**
 * Created by daniel.joppi on 12/19/14.
 */
var _ = require('lodash'),
    cclone = require('circularclone'),
    diff = require('deep-diff').diff,
    async = require('async');

const _ignoreNotFound = require('./ignore-not-found'),
    _limit = require('./limit'),
    _modifiers = require('./modifiers');

module.exports = function (dyn, trigger, items, query, db) {
    if (query.canLimit())
        items = _limit(items, query);

    var p = dyn.promise(null, null, 'consumed'),
        _load = function (key, proot, done) {
            var item = items[key];

            query.identity.set(item._id, 0, item);

            async.forEach(Object.keys(item),
                function (field, done) {
                    if (field.indexOf('___') == 0) {
                        var attr = field.substring(3),
                            _id = item[field];

                        query.identity.get(_id, '_', function (items) {
                            if (items) {
                                item[attr] = items;
                                done();
                            }
                            else
                                query.table.find({_id: _id},
                                    query.toprojection(proot[attr]),
                                    query.identity)
                                    .results(function (values) {
                                        item[attr] = values;
                                        query.identity.set(_id, '_', values);
                                        done();
                                    })
                                    .consumed(p.trigger.consumed)
                                    .error(_ignoreNotFound(done, _id, '_', query));
                        });
                    }
                    else if (field.indexOf('__') == 0) {
                        var attr = field.substring(2),
                            ptr = dyn.deref(item[field], query.table._dynamo.TableName);

                        query.identity.get(ptr._id, ptr._pos, function (loaded) {
                            if (loaded) {
                                item[attr] = loaded;
                                done();
                            }
                            else
                                db[ptr._table].findOne({_id: ptr._id, _pos: ptr._pos},
                                    query.toprojection(proot[attr]),
                                    query.identity)
                                    .result(function (value) {
                                        item[attr] = value;
                                        query.identity.set(ptr._id, ptr._pos, value);
                                        done();
                                    })
                                    .consumed(p.trigger.consumed)
                                    .error(_ignoreNotFound(done, ptr._id, ptr._pos, query));
                        });
                    }
                    else if (field == '_ref') {
                        var ptr;

                        if (query.noderef) {
                            if (!item._id)
                                item = _.extend(item, dyn.deref(item._ref, query.table._dynamo.TableName));

                            delete item['_ref'];
                            done();
                            return;
                        }

                        ptr = dyn.deref(item._ref, query.table._dynamo.TableName);

                        var _diff = diff(query.toprojection(proot), {_id: 1, _pos: 1});

                        if (_diff)
                            query.identity.get(ptr._id, ptr._pos, function (loaded) {
                                if (loaded) {
                                    item = loaded;
                                    done();
                                }
                                else
                                    db[ptr._table].findOne({_id: ptr._id, _pos: ptr._pos},
                                        query.toprojection(proot),
                                        query.identity)
                                        .result(function (loaded) {
                                            item = loaded;
                                            query.identity.set(ptr._id, ptr._pos, loaded);
                                            done();
                                        })
                                        .consumed(p.trigger.consumed)
                                        .error(_ignoreNotFound(done, ptr._id, ptr._pos, query));
                            });
                        else {
                            item = {_id: ptr._id, _pos: ptr._pos};
                            done();
                        }
                    }
                    else
                        done();
                },
                function (err) {
                    if (err) done(err);
                    else {
                        items[key] = item;
                        done();
                    }
                });
        },
        _refine = function (key, done) {
            _load(key, query.projection.root,
                function (err) {
                    var item = items[key];
                    query.identity.set(item._id, 0, item);

                    if (err)
                        done(err);
                    else
                        done();
                });

        };

    if ((query.projection.exclude || []).length > 0)
        _refine = _.wrap(_refine, function (wrapped, key, done) {
            var item = items[key];
            items[key] = item._ref ? item : _.omit(item, query.projection.exclude);

            wrapped(key,
                function (err) {
                    if (err) done(err);
                    else {
                        var item = items[key];
                        items[key] = item._ref ? _.omit(item, query.projection.exclude) : item;
                        done();
                    }
                });
        });

    _refine = _.wrap(_refine, function (wrapped, key, done) {
        wrapped(key,
            function (err) {
                if (err) done(err)
                else {
                    var item = items[key], cir = [],
                        queue = [],
                        set = function (known, clone, attr, filter) {
                            var val = _.findWhere(known, filter);

                            if (val)
                                return val;
                            else
                                queue.push(arguments);
                        };

                    item._table = query.table._dynamo.TableName;

                    item._old = cclone(item, function (field, value, clone, node, origValue, known) {
                        if (typeof field != 'string') return value;

                        if (field.indexOf('___') == 0) {
                            var attr = field.substring(3),
                                parts = value.split('$:$'),
                                _id = parts[0];

                            if (!clone[attr])
                                clone[attr] = set(known, clone, attr, {_id: _id});
                            else if (Array.isArray(clone[attr]))
                                clone[attr]._id = _id;
                        }
                        else if (field.indexOf('__') == 0) {
                            var attr = field.substring(2),
                                parts = value.split('$:$'),
                                _id = parts[0];

                            if (!clone[attr])
                                clone[attr] = set(known, clone, attr, {_id: _id, _pos: 0});
                        }
                        else if (field == '_ref') {
                            var _id, _pos = 0;

                            if (typeof value == 'string') {
                                var parts = value.split('$:$');
                                _id = parts[0];

                                if (parts[1])
                                    _pos = +parts[1];
                            }
                            else {
                                _id = value._id;
                                _pos = value._pos;
                            }

                            if (!clone[attr])
                                clone[attr] = set(known, clone, attr, {_id: _id, _pos: _pos});
                        }

                        return value;
                    });

                    queue.forEach(function (args) {
                        set.apply(null, args);
                    });

                    done();
                }
            });
    });

    async.forEach(_.range(items.length),
        _refine,
        function (err) {
            if (err)
                refiner.trigger.error(err);
            else
                trigger(_modifiers(items, query));
        });

    return p;
};