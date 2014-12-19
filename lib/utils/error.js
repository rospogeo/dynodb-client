/**
 * Created by daniel.joppi on 12/19/14.
 */
var _= require('lodash');
module.exports =  function (err) {
    return _.extend(new Error(), err);
};