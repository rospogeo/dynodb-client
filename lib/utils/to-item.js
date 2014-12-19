/**
 * Created by daniel.joppi on 12/19/14.
 */
var _value= require('./value');

module.exports = function (obj) {
    var Item= {};

    Object.keys(obj).forEach(function (key) {
        if (key!='__jsogObjectId')
            Item[key]= _value(obj[key]);
    });

    return Item;
};