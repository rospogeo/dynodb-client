/**
 * Created by daniel.joppi on 12/19/14.
 */
module.exports = function (obj) {
    obj._id = obj._id || uuid();
    obj._pos = obj._pos || 0;
    obj._rev = (obj._rev || 0) + 1;
    obj._table = table._dynamo.TableName;
    obj._refs = [];
};