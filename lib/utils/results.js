/**
 * Created by daniel.joppi on 12/19/14.
 */
var _ = require('lodash');
const _catch = require('./catch'),
    _item = require('./item');

module.exports = function (opts, query, promise, op, cb) {
    return function _iterator(count) {
        op(query, function (err, data) {
            if (data && data.ConsumedCapacity)
                promise.trigger.consumed({table: query.TableName, read: data.ConsumedCapacity.CapacityUnits, write: 0});

            if (err)
                promise.trigger.error(err);
            else {
                if (opts.count) {
                    if (data.LastEvaluatedKey) {
                        query.ExclusiveStartKey = data.LastEvaluatedKey;
                        var cnt = data.Count + (count || 0);
                        promise.trigger.progress(cnt);
                        _iterator(cnt);
                    }
                    else
                        _catch(cb)(data.Count + (count || 0));
                }
                else {
                    var results = _.collect(data.Items, _item);

                    if (!!data.LastEvaluatedKey)
                        results.next = function () {
                            query.ExclusiveStartKey = data.LastEvaluatedKey;
                            _iterator();
                        };

                    _catch(cb)(results);

                    if (!results.next)
                        promise.trigger.end();
                }
            }
        });
    };
};