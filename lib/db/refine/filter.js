/**
 * Created by daniel.joppi on 12/19/14.
 */
const _operators = require('./operators');
module.exports =  function (items, query) {

    var fieldNames = Object.keys(query.$filter), next = items.next;

    if (fieldNames.length) {
        if (query.opts.hints) console.log('client side filter'.red);

        fieldNames.forEach(function (fieldName) {
            var field = query.$filter[fieldName];
            delete query.$filter[fieldName];
            query.$filtered.push(fieldName);
            items = _.filter(items, _operators[field.op](fieldName, field.values));
        });

    }

    items.next = next;

    return items;
};