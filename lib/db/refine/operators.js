/**
 * Created by daniel.joppi on 12/19/14.
 */
var _ = require('lodash');
const _operator = require('./operator');

module.exports = {
    EQ: _operator(function (itemVal, vals) {
        return itemVal === vals[0];
    }),

    NE: _operator(function (itemVal, vals) {
        return itemVal !== vals[0];
    }),

    GT: _operator(function (itemVal, vals) {
        return itemVal > vals[0];
    }),

    GE: _operator(function (itemVal, vals) {
        return itemVal >= vals[0];
    }),

    LT: _operator(function (itemVal, vals) {
        return itemVal < vals[0];
    }),

    LE: _operator(function (itemVal, vals) {
        return itemVal <= vals[0];
    }),

    IN: _operator(function (itemVal, vals) {
        return _.contains(vals, itemVal);
    }),

    BEGINS_WITH: _operator(function (itemVal, vals) {
        return itemVal && itemVal.indexOf(vals[0]) == 0;
    }),

    CONTAINS: _operator(function (itemVal, vals) {
        return itemVal && itemVal.indexOf(vals[0]) > -1;
    }),

    REGEXP: _operator(function (itemVal, vals) {
        return !!vals[0].exec(itemVal);
    }),

    BETWEEN: _operator(function (itemVal, vals) {
        return itemVal >= vals[0] && itemVal <= vals[1];
    }),

    NULL: _operator(function (itemVal, vals) {
        return itemVal == undefined || itemVal == null;
    }),

    NOT_NULL: _operator(function (itemVal, vals) {
        return !(itemVal == undefined || itemVal == null);
    }),

    ALL: _operator(function (itemVal, vals) {
        return !_.difference(vals, itemVal).length;
    })
};