/*
 * Express request handlers to run queries against RedShift
 *
 */

'use strict';

var _ = require('lodash');
var pg = require('pg');
var queries = require('./build/js/inbox_queries');
var Q = require('q');

var conString = process.env.AWS_REDSHIFT_CONN_STRING;

/* sanitize query parameters: */
function sanitizeParams(params) {
    function sanitize(s) {
        return s.replace(/'/g,"\\'");
    }
    var sane = {};
    var keys = _.keys(params);
    keys.forEach(function (k) {
        var value = params[k];
        sane[k] = sanitize(value);
    });

    return sane;
}


/* Run a query against IA data warehouse */

function runInboxQuery(queryTemplate, queryParams) {
    var deferred = Q.defer();

    var query;
    if (typeof queryTemplate === 'function') {
        var saneParams = sanitizeParams(queryParams);
        query = queryTemplate.apply(null, [saneParams]);
    } else {
        query = queryTemplate;
    }

    pg.connect(conString, function(err, client, done) {
        function runQuery(query) {
            console.log("Executing query: ", query, ": ");
        }

        if(err) {
            console.error('error fetching client from pool', err);
            deferred.reject(new Error(err));
            return;
        }
        client.query(query, function(err, result) {
            //call `done()` to release the client back to the pool
            done();

            if(err) {
                console.error('error running query', err);
                deferred.reject(new Error(err));
                return;
            }
            deferred.resolve(result);
        });
    });

    return deferred.promise;
}

/* http GET handler for running a named query */
function getQuery(req,responseHandler) {
    if (!req.user) {
        responseHandler.status(500).json({error: "Need a valid user session to perform queries"});
        return;
    }

    var queryName = req.params.queryName;

    console.log("getQuery: req.query: ", req.query);

    var queryTemplate = queries[queryName];

    if (!queryTemplate) {
        var msg = "Unknown query '" + queryName + "'";
        responseHandler.status(500).json({error: msg});
        return;
    }

    console.log("running query: ", queryTemplate );
    runInboxQuery(queryTemplate,req.query).then(function (queryRes) {
        console.log("Successfully executed query.  Got ", queryRes.rows.length, " result rows.");
        responseHandler.json(queryRes);
    },function (err) {
        console.log("Error executing RedShift query:", err);
    });
}

module.exports.getQuery = getQuery;