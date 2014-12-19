var _ = require('lodash'),
    ret = require('ret');

const _soa = require('./db/soa'),
    _oa = require('./db/oa'),
    _buildFilter = require('./db/build-filter');

module.exports = function (dyn, opts) {
    var parser = {};

    parser.parse = function (table, modifiers, cond, projection, identity) {
        var p = dyn.promise('parsed'),
            query = {table: table, cond: cond || {}, projection: {root: {}}, opts: opts},
            _projection = function (root) {
                var proj = {include: ['_id', '_pos', '_ref'], exclude: []};

                _.keys(root).forEach(function (attr) {
                    if (root[attr].$include) {
                        proj.include.push(attr);
                        proj.include.push('__' + attr);
                        proj.include.push('___' + attr);
                    }
                    else if (root[attr].$exclude) {
                        proj.exclude.push(attr);
                        proj.exclude.push('__' + attr);
                        proj.exclude.push('___' + attr);
                    }
                });

                proj.include = _.uniq(proj.include);
                proj.exclude = _.uniq(proj.exclude);

                if (proj.include.length == 3)
                    proj.include = undefined;

                return proj;
            };

        table.name = table._dynamo.TableName;
        table.indexes = table.indexes || [];

        query.project = _projection;
        query.soa = _soa;
        query.oa = _oa;

        if (modifiers.orderby) {
            var fields = [];

            Object.keys(modifiers.orderby).forEach(function (name) {
                fields.push({name: name, dir: modifiers.orderby[name]});
            });

            query.$orderby = fields;
        }

        query.toprojection = function (root) {
            var proj = {};

            _.keys(root || {}).forEach(function (attr) {
                if (root[attr].$include)
                    proj[attr] = 1;
                else if (root[attr].$exclude)
                    proj[attr] = -1;
            });

            return proj;
        };

        if (projection && _.keys(projection).length) {
            query.identity = {};

            query.identity.get = function (_id, _pos, done) {
                done();
            };

            query.identity.set = function (item) {
            };
        }
        else {
            if (identity && identity.map)
                query.identity = identity;
            else {
                query.identity = {};
                query.identity.map = {};

                query.identity.get = function (_id, _pos, cb) {
                    var item = query.identity.map[_id + ':' + _pos];

                    if (!item) {
                        query.identity.map[_id + ':' + _pos] = true;
                        cb();
                    }
                    else if (item === true)
                        setTimeout(query.identity.get, 100, _id, _pos, cb);
                    else
                        cb(item);
                }

                query.identity.set = function (_id, _pos, item) {
                    query.identity.map[_id + ':' + _pos] = item;
                }
            }
        }

        process.nextTick(function () {

            _.extend(query, modifiers);

            if (projection) {
                _.keys(projection).every(function (attr) {
                    if (projection[attr] == 1)
                        _soa(query.projection.root, attr, {$include: true});
                    else if (projection[attr] == -1)
                        _soa(query.projection.root, attr, {$exclude: true});
                    else {
                        p.trigger.error(new Error('unknown projection value ' + JSON.stringify(projection[attr])));
                        return false;
                    }

                    return true;
                });

                _.extend(query.projection, query.project(query.projection.root));
            }

            query.$supported = _buildFilter(query);
            query.$returned = 0;
            query.$filtered = [];
            query.window = query.window || 50;

            query.filterComplete = function () {
                return Object.keys(query.$filter).length == 0;
            };

            query.sortComplete = function () {
                return !(query.orderby && !query.sorted);
            };

            query.limitComplete = function () {
                return !(query.limit !== undefined && !query.limited);
            };

            query.skipComplete = function () {
                return !(query.skip !== undefined && !query.skipped);
            };

            query.canLimit = function () {
                return !query.limited
                    && query.sortComplete()
                    && query.filterComplete();
            };

            query.canSkip = function () {
                return !query.skipped
                    && query.sortComplete()
                    && query.filterComplete();
            };

            query.canCount = function () {
                return query.filterComplete()
                    && query.limitComplete()
                    && query.skipComplete();
            };

            query.finderProjection = function () {
                if (query.projection.include) {
                    // if i cannot filter them i project them to the refiner for client-side filtering
                    var toproject = _.difference(_.keys(query.$filter), query.projection.include) || [];
                    return _.union(query.projection.include, toproject);
                }
                else
                    return undefined;
            };

            p.trigger.parsed(query);

        });

        return p;
    };

    return parser;
};
