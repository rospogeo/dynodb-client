/**
 * Created by daniel.joppi on 12/19/14.
 */
var _ = require('lodash');

module.exports = function (val) {
    var type = typeof val;

    if (type == 'object' && val instanceof Buffer)
        return {'B': val.toString('base64')};
    else if (type == 'object' && val instanceof Date)
        return {'S': val.toISOString()};
    else if (type == 'object' && Array.isArray(val)) {
        if (val.length > 0) {
            var etype = typeof val[0];

            if (etype == 'object' && val[0] instanceof Buffer)
                return {
                    'BS': _.collect(val, function (v) {
                        return v.toString('base64')
                    })
                };
            else if (etype == 'number')
                return {
                    'NS': _.collect(val, function (v) {
                        return v + '';
                    })
                };
            else if (etype == 'string')
                return {
                    'SS': _.collect(val, function (v) {
                        return v + '';
                    })
                };
            else
                throw new Error('unknown type of array value: ' + etype);
        }
        else
            throw new Error('empty array');
    }
    else if (type == 'number')
        return {'N': val + ''};
    else if (type == 'string')
        return {'S': val};
    else if (type == 'boolean')
        return {'N': (val ? 1 : 0) + ''};
    else
        throw new Error('unknown type of value: ' + type);
};