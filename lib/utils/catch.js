/**
 * Created by daniel.joppi on 12/19/14.
 */
module.exports = function (fn) {
    return function () {
        try {
            fn.apply(null,arguments);
        } catch (ex) {
            console.log(ex, ex.stack);
        }
    };
};