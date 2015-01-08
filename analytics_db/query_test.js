/*
 * Test of queries produced by running inbox_queries through Traceur compiler
 */
var pg = require('pg');
var queries = require('./build/js/inbox_queries');
var pgutils = require('./pgutils');
var argv = require('optimist')
    .usage('Usage: $0 -u [uid]')
    .demand('u')
    .argv;
var ctx = queries.queryContext({user_id: argv.u});

var conString = process.env.AWS_REDSHIFT_FRONTEND_STRING;

var queries = [
    queries.rawMessagesCount(ctx),
    queries.topCorrespondents(ctx)
];

var queryPromise = pgutils.qpg(conString,pgutils.mkQuerySequence(queries));

queryPromise.then(function (state) {
    var resultRows = state.results.map(function (res) { return res.rows; } );
    console.log("===> ", resultRows);
    return null;
    pg.end();
},function (err) {
    console.log("\n\n*** Error executing query: \n", err, "\n", err.stack);
    pg.end();
});
