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

var queries = ['vacuum',
    queries.rebuildCorrespondentTables(ctx,userRealName,userEmailAddrs),
    queries.createCIDMessagesView(ctx), 
    queries.createCIDMessagesRecipients(ctx),
    queries.createDirectToUserMessages(ctx,userRealName),
    queries.createFromUserMessagesRecips(ctx,userRealName),
    'vacuum; analyze'
];

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
