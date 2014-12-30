/*
 * utilities for running queries against Postgres
 */

var pg = require('pg');
var Q = require('q');

/* TODO: This interface is awkward, but allows us to chain queries together to run sequentially.
 *
 * The issue is the messiness with needing to call done() to clean up in qpg.
 */

/*
 * given a SQL query, return a function from ClientState -> Promise<ClientState>
 * that can be passed to qpg.
 * ClientState is an object with two fields:
 *   client -- the pg client against which we can return
 *   results -- An array of query results collected so far
 */
function mkQueryAction(query) {
    function runFn(state) {    
        var deferred = Q.defer();
        console.log("About to run query: ", query);
        state.client.query(query,function (err,res) {
            if (err) {
                deferred.reject(err);
            } else {
                var newState = {client: state.client, results: state.results.concat(res)}
                deferred.resolve(newState);
            }
        });

        return deferred.promise;
    }
    return runFn;
}

/*
 * given an array of queries, produce a single action fn that will run all queries in sequence:
 */
function mkQuerySequence(queries) {
    // map queries into functions of the form ClientState -> Promise<ClientState>
    var runFns = queries.map(mkQueryAction);

    function runSeq(state) {
        return runFns.reduce(function (statePromise,runFn) {
            return statePromise.then(runFn);
        }, Q(state));    
    }
    return runSeq;     
} 


/*
 * pg.connect() wrapped in a Q promise
 *
 * arguments: connection string
 * afn - A function from ClientState -> Promise<ClientState>
 *
 * returns: Promise<ClientState>
 */
function qpg(conString,afn) {
    var deferred = Q.defer();

    pg.connect(conString, function(err, client, done) {
        if (err) {
            deferred.reject(err);
        } else {
            var state = {client: client, results: []};
            var actionPromise = afn(state);
            deferred.resolve(actionPromise.finally(done));
        }
    });
    return deferred.promise; 
}

module.exports.qpg = qpg;
module.exports.mkQueryAction = mkQueryAction;
module.exports.mkQuerySequence = mkQuerySequence;