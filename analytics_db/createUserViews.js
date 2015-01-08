/*
 * build base views for a user, and grant access to frontend.
 * Note: This will run as awsuser, NOT as ai_frontend!
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

var conString = process.env.AWS_REDSHIFT_CONN_STRING;

var queries = ['vacuum',
    queries.createBaseViews(ctx),
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
