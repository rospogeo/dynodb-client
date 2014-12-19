/**
 * Created by daniel.joppi on 12/19/14.
 */
module.exports = function (items, query) {
    if (query.limit && (items.length + query.$returned > query.limit) && !query.limited) {
        if (!query.count && query.opts.hints) console.log('client side limit'.red);
        items = items.slice(0, query.limit - query.$returned);
        query.limited = true;
    }

    if (query.skip && !query.skipped) {
        if (query.opts.hints) console.log('client side skip'.red);

        items = items.slice(query.skip);
        query.skipped = true;
    }

    return items;
};