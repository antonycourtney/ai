/*
 * gensql -- node script to expand a name SQL template from inbox_queries
 *           and emit the SQL to stdout
 *
 */

var _ = require('lodash');
var queries = require('./build/js/inbox_queries');
var tqs = require('./build/js/test_queries');
_.extend(queries,tqs);

function main(argv) {
    // N.B.: In node indices 0 and 1 for 'node' and name of JS file
    if (argv.length < 3) {
        console.error("usage: getsql [query]");
    }
    var templateName = argv[2];
    var template = queries[templateName];
    if (!template) {
        console.error("Unknown query template: '" + templateName + "'");
        console.error("Available query templates: ", _.keys(queries));
        process.exit(1);
    }
    var query;
    if (argv.length > 3) {
        // slice additional args and apply template:
        var targs = argv.slice(3);
        query = template.apply(null,targs);
    } else {
        if (typeof template === 'function') {
            console.error("template '" + templateName + "' is a function, must supply additional arguments" );
            process.exit(1);
        }
        query = template;
    }
    console.log(query);
}

main(process.argv);
