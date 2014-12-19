/**
 * Created by daniel.joppi on 12/19/14.
 */
const _oa = require('../oa');

module.exports = function (cmp) {
    return function (fieldName, vals) {
        return function (item) {
            return cmp(_oa(item, fieldName), vals);
        };
    };
};