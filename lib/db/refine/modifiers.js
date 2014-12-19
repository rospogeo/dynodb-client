/**
 * Created by daniel.joppi on 12/19/14.
 */
const _limit = require('./limit'),
    _filter =require('./filter'),
    _sort = require('./sort'),
    _notfound = require('./not-found');

module.exports = function (items, query) {
    if (items.refine)
        items = items.refine(items);

    items = _notfound(items);
    items = _filter(items, query);
    items = _sort(items, query);
    items = _limit(items, query);

    return items;
};