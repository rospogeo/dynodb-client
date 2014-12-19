/**
 * Created by daniel.joppi on 12/19/14.
 */
var _ = require('lodash');

module.exports = function (consume) {
    return function (cons) {
        _.keys(cons).forEach(function (table) {
            var c, tcons = cons[table];

            if (!(c = consume[table]))
                c = consume[table] = {read: 0, write: 0};

            c.read += tcons.read;
            c.write += tcons.write;
        });
    };
};