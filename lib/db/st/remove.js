/**
 * Created by daniel.joppi on 12/19/14.
 */
var _ = require('lodash');

module.exports = function(ops, gops) {
    return function (item) {
        ops.push({op: 'del', item: {_id: item._id, _pos: item._pos}});

        table.indexes.forEach(function (index) {
            var iops = index.update(item, 'del') || {};

            _.keys(iops).forEach(function (table) {
                var tops = gops[table] = gops[table] || [];
                tops.push.apply(tops, _.collect(iops[table], function (op) {
                    op.index = true;
                    return op;
                }));
            });
        });
    };
};