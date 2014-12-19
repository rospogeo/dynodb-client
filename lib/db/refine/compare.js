/**
 * Created by daniel.joppi on 12/19/14.
 */
module.exports = function (x, y) {
    if (x === y)
        return 0;

    return x > y ? 1 : -1;
};