/**
 * Created by daniel.joppi on 12/19/14.
 */
module.exports = function (done, _id, _pos, query) {
    return function (err) {
        if (err.code == 'notfound') {
            query.identity.set(_id, _pos, undefined);
            done();
        }
        else
            done(err);
    };
};