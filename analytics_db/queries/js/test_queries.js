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

var acEmailAddrs = process.env.TEST_USER_ADDRS.split(',');
var acRealName = process.env.TEST_USER_REAL_NAME;

var acTestUserRecips = queries.userRecipients(acEmailAddrs);
var acTestBestCorrespondentNames = queries.bestCorrespondentNames(acRealName,acEmailAddrs);
var acTestCorrespondentEmails = queries.correspondentEmails(acRealName,acEmailAddrs);
var acRankedNamePairs= queries.rankedNamePairs(acEmailAddrs);
var acTestDistinctNamesList = queries.distinctNamesList(acRealName,acEmailAddrs);
var acTestCorrNames = queries.correspondentNames(acRealName,acEmailAddrs);
var acDups = findDups(['emailAddress'],'( ' + acTestCorrespondentEmails + ' )');

module.exports.acTestDistinctNamesList = acTestDistinctNamesList;
module.exports.acTestCorrNames = acTestCorrNames;
module.exports.acDups = acDups;
module.exports.acTestUserRecips = acTestUserRecips;
module.exports.acTestBestCorrespondentNames = acTestBestCorrespondentNames;
module.exports.acTestCorrespondentEmails = acTestCorrespondentEmails;
module.exports.acRankedNamePairs = acRankedNamePairs;