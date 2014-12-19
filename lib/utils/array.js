/**
 * Created by daniel.joppi on 12/19/14.
 */
module.exports = function (val) {
    return Array.isArray(val) ? val : [val];
};