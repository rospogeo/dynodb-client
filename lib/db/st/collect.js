/**
 * Created by daniel.joppi on 12/19/14.
 */
module.exports = function (consume) {
    return function (cons) {
        var c;

        if (cons.table) {
            if (!(c = consume[cons.table]))
                c = consume[cons.table] = {read: 0, write: 0};

            c.read += cons.read;
            c.write += cons.write;
        }
        else
            _.keys(cons).forEach(function (table) {
                var c, tcons = cons[table];

                if (!(c = consume[table]))
                    c = consume[table] = {read: 0, write: 0};

                c.read += tcons.read;
                c.write += tcons.write;
            });
    };
};