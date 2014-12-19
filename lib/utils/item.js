/**
 * Created by daniel.joppi on 12/19/14.
 */
var _= require('lodash');

module.exports = function (Item) {
    var obj= {};

    Object.keys(Item).forEach(function (key) {
        if (key=='__jsogObjectId')
        { /*ignore*/ }
        else
        if (Item[key].S !== undefined)
            obj[key]= Item[key].S;
        else
        if (Item[key].N !== undefined) {
            if (Item[key].N.indexOf('.')>-1)
                obj[key]= parseFloat(Item[key].N);
            else
                obj[key]= parseInt(Item[key].N);
        }
        else
        if (Item[key].B !== undefined)
            obj[key]= new Buffer(Item[key].B,'base64');
        else
        if (Item[key].SS !== undefined)
            obj[key]= Item[key].SS;
        else
        if (Item[key].NS !== undefined)
            obj[key]= _.collect(Item[key].NS,function (n) {
                if (n.indexOf('.')>-1)
                    return parseFloat(n);
                else
                    return parseInt(n);
            });
        else
        if (Item[key].BS !== undefined)
            obj[key]= _.collect(Item[key].BS,function (b) { return new Buffer(b,'base64'); });
    });

    return obj;
};