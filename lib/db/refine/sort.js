/**
 * Created by daniel.joppi on 12/19/14.
 */
const _compare = require('./compare');

module.exports = function (items, query) {
    if (query.orderby && !query.sorted) {
        if (query.opts.hints) console.log('client side sort'.red);

        var fields = query.$orderby;

        items.sort(function (x, y) {
            var retval;

            fields.some(function (field) {
                var fx = query.oa(x, field.name),
                    fy = query.oa(y, field.name);

                if (fx != fy) {
                    retval = _compare(fx, fy) * field.dir;
                    return true;
                }
            });

            return retval;
        });
    }

    return items;
};