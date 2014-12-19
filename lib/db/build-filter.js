/**
 * Created by daniel.joppi on 12/19/14.
 */
var _= require('lodash'),
    ret= require('ret');

module.exports = function (query) {
    var cond = query.cond,
        filter = query.$filter = {},
        canFind = true;

    if (cond.$text) {
        query.$text = cond.$text;
        cond = query.cond = _.omit(query.cond, '$text');
    }

    Object.keys(cond).every(function (field) {
        var val = cond[field], type = typeof val,
            _field = function (field, val, op) {
                filter[field] = {
                    values: val === undefined ? [] : (Array.isArray(val) ? val : [val]),
                    op: op
                };
            },
            _regexp = function (field, val) {
                var tks = ret(val.source),
                    _chars = function (start) {
                        return _.collect(tks.stack.slice(start),
                            function (tk) {
                                return String.fromCharCode(tk.value);
                            })
                            .join('');
                    };

                if (tks.stack
                    && tks.stack[0]
                    && tks.stack[0].type == ret.types.POSITION
                    && tks.stack[0].value == '^'
                    && !_.filter(tks.stack.slice(1), function (tk) {
                        return tk.type != ret.types.CHAR;
                    }).length) {
                    var val = _chars(1);
                    if (val != '')
                        _field(field, val, 'BEGINS_WITH');
                }
                else if (tks.stack
                    && !_.filter(tks.stack, function (tk) {
                        return tk.type != ret.types.CHAR;
                    }).length) {
                    var val = _chars(0);
                    if (val != '')
                        _field(field, val, 'CONTAINS');
                }
                else
                    _field(field, val, 'REGEXP');
            };

        if (type == 'object') {
            if (Array.isArray(val))
                return canFind = false;
            else {
                if (val instanceof RegExp)
                    _regexp(field, val);
                else if (val.$gte !== undefined && val.$lte !== undefined)
                    _field(field, [val.$gte, val.$lte], 'BETWEEN');
                else if (val.$ne !== undefined)
                    _field(field, val.$ne, 'NE');
                else if (val.$gt !== undefined)
                    _field(field, val.$gt, 'GT');
                else if (val.$lt !== undefined)
                    _field(field, val.$lt, 'LT');
                else if (val.$gte !== undefined)
                    _field(field, val.$gte, 'GE');
                else if (val.$lte !== undefined)
                    _field(field, val.$lte, 'LE');
                else if (val.$in !== undefined)
                    _field(field, val.$in, 'IN');
                else if (val.$all !== undefined)
                    _field(field, val.$all, 'ALL');
                else if (val.$exists !== undefined)
                    _field(field, undefined, val.$exists ? 'NOT_NULL' : 'NULL');
                else
                    return canFind = false;
            }
        }
        else
            _field(field, val, 'EQ');

        return true;
    });

    return canFind;
};