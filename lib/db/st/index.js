/**
 * Created by daniel.joppi on 12/19/14.
 */
module.exports = function (obj) {
    table.indexes.forEach(function (index) {
        var iops = index.update(obj, 'put') || {};

        _.keys(iops).forEach(function (table) {
            var tops = gops[table] = gops[table] || [];
            tops.push.apply(tops, _.collect(iops[table], function (op) {
                op.index = true;
                return op;
            }));
            tops.index = true;
        });
    });
};