/*
 * Express request handlers to run queries against RedShift
 *
 */

'use strict';

var _ = require('lodash');
var pg = require('pg');
var pgutils = require('../../analytics_db/pgutils');
var queries = require('../../analytics_db/build/js/inbox_queries');
var Q = require('q');
var models = require('./models.js');

var conString = process.env.AWS_REDSHIFT_FRONTEND_STRING;

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

function runInboxQuery(user,queryTemplate, queryParams) {
    var deferred = Q.defer();

    var ctx = queries.queryContext({user_id: user.id});
    var saneParams = [ctx];
    if (_.keys(queryParams).length > 0)
        saneParams.push( sanitizeParams(queryParams) );

    var query = queryTemplate.apply(null, saneParams);

    console.log("runInboxQuery: \n", query);

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

    var checkDerivedTables = queries['checkDerivedTables'];
    var rebuildDerivedTables = queries['rebuildDerivedTables'];

    var conString = process.env.AWS_REDSHIFT_FRONTEND_STRING;
    var ctx = queries.queryContext({user_id: req.user.id});

    // Get the user's real name and all email addresses
    var userRealName = req.user.attributes['real_name'];
    var idPromise = models.Identity.where({user_id: req.user.id}).fetchAll().then(function (identities) {

        var userEmailAddrs = identities.models.map(function(value, index, arr) { return value.attributes['email']; });

        // Check if we need to create the derived tables. If so, we do so and record this fact in the user session
        if (!req.session.derivedTables) {

            console.log("Checking for derived tables");
            runInboxQuery(req.user,checkDerivedTables,req.query).then(function (queryRes) {

                console.log("Checked for derived tables, got: ", queryRes);

                // If the derived table wasn't found, recreate them all
                if (queryRes.rows[0]['count'] == '0') {

                    console.log("Rebuilding derived tables");
                    pgutils.qpg(conString,pgutils.mkQuerySequence(rebuildDerivedTables(ctx, userRealName, userEmailAddrs))).then(function (queryRes) {

                        console.log("Rebuilt derived tables, got: ", queryRes);

                        // We've created the derived tables, so no need to check for them again this session
                        req.session.derivedTables = true;
    
                        // Now that we've created the derived tables, go ahead with the original query
                        return getQueryWithDerivedTables(req, responseHandler);
                    });
                } else {
                    
                    // We've checked for the derived tables presence and found them, record this and go ahead with the original query
                    req.session.derivedTables = true;

                    return getQueryWithDerivedTables(req, responseHandler);
                }

            });

        } else {
            // We don't need to create the derived tables, so just go ahead with the requested query
            return getQueryWithDerivedTables(req, responseHandler);
        }

    });
    return;

}

function getQueryWithDerivedTables(req, responseHandler)
{
    var queryName = req.params.queryName;

    console.log("getQueryWithDerivedTables: queryName: ", queryName, "req.query: ", req.query);

    var queryTemplate = queries[queryName];

    if (!queryTemplate) {
        var msg = "Unknown query '" + queryName + "'";
        responseHandler.status(500).json({error: msg});
        return;
    }

    console.log("running query: ", queryTemplate );
    runInboxQuery(req.user,queryTemplate,req.query).then(function (queryRes) {
        console.log("Successfully executed query.  Got ", queryRes.rows.length, " result rows.");
        responseHandler.json(queryRes);
    },function (err) {
        console.log("Error executing RedShift query:", err);
        responseHandler.status(500).json({error: "Error executing Redshift query: " + err});

    });
}

module.exports.getQuery = getQuery;