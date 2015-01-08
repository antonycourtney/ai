/*
 * gensql -- node script to expand a name SQL template from inbox_queries
 *           and emit the SQL to stdout
 *
 */

var _ = require('lodash');
var queries = require('./build/js/inbox_queries');
var tqs = require('./build/js/test_queries');
_.extend(queries,tqs);

var optimist = require('optimist');


function main() {

    var argv = optimist
        .usage('Usage: $0 -u [uid] query [params...]')
        .demand('u')
        .demand(1)
        .argv;
    posArgs = argv._;
    var ctx = queries.queryContext({user_id: argv.u});

    var templateName = posArgs.shift();
    var template = queries[templateName];
    if (!template) {
        console.error("Unknown query template: '" + templateName + "'");
        console.error("Available query templates: ", _.keys(queries));
        process.exit(1);
    }

    var targs = [ctx];
    targs = targs.concat(posArgs);
    query = template.apply(null,targs);
    console.log(query);
}

main();
