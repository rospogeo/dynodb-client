/**
 * Created by daniel.joppi on 12/19/14.
 */
var _ = require('lodash');

const _hashrange = require('./hash-range'),
    _index = require('./index');

module.exports = function(ops, gops) {
    const _remove = require('./remove')(ops, gops);

    return function (obj, isCreate, isAggregate) {
        var _keys = _.keys(obj),
            _omit = ['_old', '_table'],
            diffs = diff(obj._old || {},
                obj,
                function (path, key) {
                    return key == '_old';
                });

        if (!diffs || diffs.length == 0 || (obj._old || {_rev: 0})._rev < obj._rev
        ) return;

        if (obj._table && obj._table != table._dynamo.TableName) {
            db[obj._table].save(obj);
            return;
        }

        _hashrange(obj);

        _keys.forEach(function (key) {
            if (key.indexOf('___') == 0) {
                if (!_isobjectarr(obj[key.substring(3)])) {
                    _omit.push(key);
                    return;
                }
            }
            else if (key.indexOf('__') == 0) {
                if (!_isobject(obj[key.substring(2)])) {
                    _omit.push(key);
                    return;
                }
            }

            var type = typeof obj[key];

            if (type == 'object' && !_.contains(['_old', '_refs'], key)) {
                var desc = obj[key];

                if (desc == null)
                    _omit.push(key);
                else if (desc instanceof Date) { /* let dyn convert */
                }
                else if (_.keys(desc).length == 0)
                    _omit.push(key);
                else if (Array.isArray(desc)) {
                    if (desc.length) {
                        if (typeof desc[0] == 'object') {
                            var _id = obj['___' + key] = obj['___' + key] || uuid();

                            if (obj._old) {
                                var old = obj._old[key];

                                if (old && old.length > desc.length)
                                    old.forEach(function (oitem, idx) {
                                        if (oitem._id == _id) {
                                            if (!_.findWhere(desc, {_pos: oitem._pos}))
                                                _remove(oitem);
                                        }
                                        else {
                                            var elem = _.findWhere(desc, {
                                                _id: oitem._id,
                                                _pos: oitem._pos
                                            });

                                            if (!elem || elem != desc[idx])
                                                _remove({
                                                    _id: _id,
                                                    _pos: idx,
                                                    _ref: dyn.ref(oitem)
                                                });
                                        }
                                    });
                            }

                            desc.forEach(function (val, pos) {
                                if (val._id && val._id != _id) {
                                    _save(val);
                                    _save({_id: _id, _pos: pos, _ref: dyn.ref(val)});
                                    obj._refs.push(val._id);
                                }
                                else {
                                    val._id = _id;

                                    if (!isNaN(val._pos) && val._pos != pos) {
                                        delete val['_old'];
                                        delete val['_rev'];
                                        _remove(val);
                                    }

                                    val._pos = pos;
                                    _save(val);
                                }
                            });

                            _omit.push(key);
                        }
                    }
                    else {
                        var _id = obj['___' + key];

                        if (_id && obj._old[key].length)
                            obj._old[key].forEach(_remove);

                        _omit.push(key);
                        _omit.push('___' + key);
                    }
                }
                else {
                    _save(desc);
                    obj['__' + key] = dyn.ref(desc);
                    obj._refs.push(desc._id);
                    _omit.push(key);
                }
            }
            else if (type == 'string' && !obj[key])
                _omit.push(key);
            else if (type == 'number' && isNaN(obj[key]))
                _omit.push(key);
        });

        if (!obj._refs.length)
            delete obj['_refs'];
        else
            obj._refs = _.uniq(obj._refs);

        _index(obj); // index after _ fields are set so they are indexable too

        var op = {op: 'put', item: obj, omit: _omit, isCreate: isCreate};

        if (isAggregate)
            ops.unshift(op); // let the aggregate op came first of "contained" objects, so that the aggrgate version protects the rest
        else
            ops.push(op);
    };
};