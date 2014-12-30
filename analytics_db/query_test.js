/*
 * Test of queries produced by running inbox_queries through Traceur compiler
 */
var pg = require('pg');
var queries = require('./build/js/inbox_queries');

// var conString = process.env.PG_CONN_STRING;
var conString = process.env.AWS_REDSHIFT_CONN_STRING;



pg.connect(conString, function(err, client, done) {
    function runQuery(query) {
        console.log("Executing query: ", query, ": ");
        client.query(query, function(err, result) {
            //call `done()` to release the client back to the pool
            done();

            if(err) {
                return console.error('error running query', err);
            }
            console.log("\n==>");
            console.log(result.rows);
            //output: 1
        });
    }

    if(err) {
        return console.error('error fetching client from pool', err);
    }
    runQuery(queries.rawMessagesCount);
    runQuery(queries.topCorrespondents);
});
