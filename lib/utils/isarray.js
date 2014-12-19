/**
 * Created by daniel.joppi on 12/19/14.
 */
module.exports = function (v) {
    return typeof v == 'object' && Array.isArray(v) && v.length > 0 && typeof v[0] == 'object';
};