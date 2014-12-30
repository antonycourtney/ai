/*
 * routines to check various invariants on tables in a SQL db
 */

'require strict';
var _ = require('lodash');
var assert = require('assert');
var pg = require('pg');
var Q = require('q');

/*
 * run two queries and check results for equality using deepEquals
 */
function assertEqualQueries(client,q1,q2,message) {
    console.log("verifying ", message);
    console.log("q1: ", q1);
    console.log("q2: ", q2);

    var p1 = Q.nfapply(client.query.bind(client),[q1]);
    var p2 = Q.nfapply(client.query.bind(client),[q2]);

    return Q.all([p1,p2]).spread(function (r1,r2) {
        assert.deepEqual(r1.rows,r2.rows,message);
        console.log("==> ok\n\n");
        return client;  // for easy sequential chaining
    },function (err) {
        console.error("Error executing queries: ", err, err.stack);
        throw err;
    });
}

/*
 * generate an invariant checker that will verify that given key
 * is a primary key for the specified table or query:
 */
function isPrimaryKey(key,table,options) {
    var keyCols = key.join(', ');
    var q1=`
    select count(*)
    from (select distinct ${keyCols} from ${table} t)`;
    var q2=`
    select count(*)
    from ${table} t`;

    var message=`${keyCols} is a primary key of `;
    if (options && options.queryName) {
        message += `query '${options.queryName}'`;
    } else {
        message += `${table}`;
    }

    function checker(client) {
        return assertEqualQueries(client,q1,q2,message);
    }
    return checker;
}

/*
 * generate a checker that will verify that a given column
 * only appears in lower case form
 */
function isLowerColumn(column,table,options) {
    var q1=`
    select count(*)
    from (select distinct ${column} from ${table} t)`;
    var q2=`
    select count(*)
    from (select distinct lower(${column}) from ${table} t)`;

    var message=`${column} is all lower case in `;
    if (options && options.queryName) {
        message += `query '${options.queryName}'`;
    } else {
        message += `${table}`;
    }

    function checker(client) {
        return assertEqualQueries(client,q1,q2,message);
    }
    return checker;    
}

/*
 * run an array of checker functions (which are functions from client to Promise<Client>)
 */
function runChecks(conString,checks) {
    pg.connect(conString, function(err, client, done) {
      if(err) {
        return console.error('error fetching client from pool', err);
      }
      var allChecks = checks.reduce(function (clientPromise,nextFunc) {
        return clientPromise.then(nextFunc);
      }, Q(client));
      allChecks.then(function (c) {
        done(); // return client to client pool
      },function (err) {
        console.log("runChecks: error in promise chain: ", err, err.stack);
      });
  });
}

module.exports.isPrimaryKey = isPrimaryKey;
module.exports.isLowerColumn = isLowerColumn;
module.exports.runChecks = runChecks;