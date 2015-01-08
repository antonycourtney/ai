/*
 * some test queries
 */
'use strict';

var queries = require('./inbox_queries');

// find counter-examples to some candidate primary key:
function findDups(key,table) {
    var keyCols = key.join(', ');
    var q1=`
    select c.*
    from (
      select ${keyCols},count(*) as keyCount
      from ${table} t
      group by ${keyCols}) c
    where c.keyCount > 1`

    var q2=`
    select t.*
    from ${table} t natural join (${q1}) dk`;

    return q2;
}

var tu_EmailAddrs = process.env.TEST_USER_ADDRS.split(',');
var tu_RealName = process.env.TEST_USER_REAL_NAME;

// tu_ stands for "TEST_USER"
// useful during development
var tu_UserRecips = (ctx) => queries.userRecipients(ctx,tu_EmailAddrs);
var tu_BestCorrespondentNames = (ctx) => queries.bestCorrespondentNames(ctx,tu_RealName,tu_EmailAddrs);
var tu_CorrespondentEmails = (ctx) => queries.correspondentEmailsQuery(ctx,tu_RealName,tu_EmailAddrs);
var tu_RankedNamePairs = (ctx) => queries.rankedNamePairs(tu_EmailAddrs);
var tu_DistinctNamesList = (ctx) => queries.distinctNamesList(ctx,tu_RealName,tu_EmailAddrs);
var tu_CorrNames = (ctx) => queries.correspondentNamesQuery(ctx,tu_RealName,tu_EmailAddrs);
var tu_Dups = (ctx) => findDups(['emailAddress'],'( ' + tu_CorrespondentEmails(ctx) + ' )');

module.exports.tu_DistinctNamesList = tu_DistinctNamesList;
module.exports.tu_CorrNames = tu_CorrNames;
module.exports.tu_Dups = tu_Dups;
module.exports.tu_UserRecips = tu_UserRecips;
module.exports.tu_BestCorrespondentNames = tu_BestCorrespondentNames;
module.exports.tu_CorrespondentEmails = tu_CorrespondentEmails;
module.exports.tu_RankedNamePairs = tu_RankedNamePairs;