/**
 * Created by daniel.joppi on 12/19/14.
 */
module.exports = function (o, s, v) {
    s = s.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
    s = s.replace(/^\./, '');           // strip a leading dot
    var a = s.split('.'),
        prop = a.pop();

    while (a.length) {
        var n = a.shift();
        o = o[n] || (o[n] = {});
    }

    o[prop] = v;
};