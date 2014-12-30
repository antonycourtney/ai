/*
 * Run some queries against RedShift to ensure that certain crucial invariants hold
 */

var _ = require('lodash');
var queries = require('./build/js/inbox_queries');
var tq = require('./build/js/test_queries');
var ic = require('./build/js/invariant_checks');

var conString = process.env.AWS_REDSHIFT_CONN_STRING;

function mkQueryTable(qs) {
    return '( ' + qs + ' )';
}

var allChecks = [
    ic.isPrimaryKey(['messageId'],'messages'),

    ic.isPrimaryKey(['correspondentName'],'correspondentNames'),
    ic.isPrimaryKey(['correspondentId'],'correspondentNames'),

    ic.isPrimaryKey(['emailAddress','correspondentName'],mkQueryTable(queries.fromAddressNamePairs),
        {queryName: 'fromAddressNamePairs' }),

    ic.isPrimaryKey(['emailAddress'],mkQueryTable(queries.distinctToOnlyAddrs),
        {queryName: 'distinctToOnlyAddrs' }),

    ic.isLowerColumn(['emailAddress'],mkQueryTable(queries.toOnlyNamePairs),
        {queryName: 'toOnlyNamePairs'}),

    ic.isPrimaryKey(['userRecipientEmailAddress'],mkQueryTable(tq.acTestUserRecips),
        {queryName: 'acTestUserRecips'}),
    ic.isLowerColumn(['userRecipientEmailAddress'],mkQueryTable(tq.acTestUserRecips),
        {queryName: 'acTestUserRecips'}),

    ic.isPrimaryKey(['emailAddress','correspondentName'],mkQueryTable(queries.allNamePairs),
        {queryName: 'allNamePairs' }),
    ic.isLowerColumn(['emailAddress'],mkQueryTable(queries.allNamePairs),
        {queryName: 'allNamePairs' }),

    ic.isPrimaryKey(['emailAddress'], mkQueryTable(tq.acTestBestCorrespondentNames),
        {queryName: 'acTestBestCorrespondentNames'}),
    ic.isLowerColumn(['emailAddress'],mkQueryTable(tq.acTestBestCorrespondentNames),
        {queryName: 'acTestBestCorrespondentNames'}),

    ic.isPrimaryKey(['emailAddress'], mkQueryTable(tq.acTestCorrespondentEmails),
        {queryName: 'acTestCorrespondentEmails'}),
    ic.isLowerColumn(['emailAddress'],mkQueryTable(tq.acTestCorrespondentEmails),
        {queryName: 'acTestCorrespondentEmails'}),

    ic.isPrimaryKey(['emailAddress'],'correspondentEmails'),
    ic.isLowerColumn(['emailAddress'], 'correspondentEmails')
];

ic.runChecks(conString,allChecks);