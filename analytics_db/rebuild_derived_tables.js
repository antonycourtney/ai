/*
 * rebuild derived tables and views on RedShift
 */
'use strict';

var _ = require('lodash');
var queries = require('./build/js/inbox_queries');
var pg = require('pg');
var pgutils = require('./pgutils');
var Q = require('q');
var assert = require('assert');
var argv = require('optimist')
    .usage('Usage: $0 -u [uid]')
    .demand('u')
    .argv;
var ctx = queries.queryContext({user_id: argv.u});

var conString = process.env.AWS_REDSHIFT_FRONTEND_STRING;

var userEmailAddrs = process.env.TEST_USER_ADDRS.split(',');
var userRealName = process.env.TEST_USER_REAL_NAME;
assert(userRealName,"TEST_USER_REAL_NAME env var must be defined");

var rebuild_tables_query = queries.rebuildCorrespondentTables(ctx,userRealName,userEmailAddrs);

var queries = ['vacuum',
    // rebuild_tables_query, 
    queries.createCIDMessagesView(ctx), 
    queries.createCIDMessagesRecipients(ctx),
    queries.createDirectToUserMessages(ctx,userRealName),
    queries.createFromUserMessagesRecips(ctx,userRealName),
    'vacuum; analyze'
    ];

// var rebuild_tables_query = queries.rawMessagesCount;
// var rebuild_tables_query = queries.bestCorrespondentNames(userRealName,userEmailAddrs);

var queryPromise = pgutils.qpg(conString,pgutils.mkQuerySequence(queries));

queryPromise.then(function (state) {
    var resultRows = state.results.map(function (res) { return res.rows; } );
    console.log("===> ", resultRows);
    pg.end();
    return null;
},function (err) {
    console.log("Error executing query: ", err, err.stack);
    pg.end();
});

/*
function rebuild_tables(conString) {
    var rebuild_query = queries.rebuildCorrespondentTables(userRealName,userEmailAddrs);

    pg.connect(conString, function(err, client, done) {
      if(err) {
        return console.error('error fetching client from pool', err);
      }
      console.log("Connected, about to run query: ", rebuild_query);
      client.query( rebuild_query, function(err, result) {
        //call `done()` to release the client back to the pool
        done();

        if(err) {
          return console.error('error running query', err);
        }
        console.log("===> ", result.rows);
        //output: 1
      });
  });
}

rebuild_tables(conString);

*/