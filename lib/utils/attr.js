/**
 * Created by daniel.joppi on 12/19/14.
 */
const _value= require('./value');

module.exports = function (o) {
    var obj= {};

    obj[o.attr]= _value(o.value);

    return obj;
};