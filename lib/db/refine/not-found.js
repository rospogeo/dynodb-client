/**
 * Created by daniel.joppi on 12/19/14.
 */
var _ = require('lodash');

module.exports = function (items) {
    var ret = _.filter(items, function (i) {
        return !!i._id;
    });

    ret.next = items.next;

    return ret;
};