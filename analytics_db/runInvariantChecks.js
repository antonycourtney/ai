/*
 * Run some queries against RedShift to ensure that certain crucial invariants hold
 */

var _ = require('lodash');
var queries = require('./build/js/inbox_queries');
var tq = require('./build/js/test_queries');
var ic = require('./build/js/invariant_checks');
var argv = require('optimist')
    .usage('Usage: $0 -u [uid]')
    .demand('u')
    .argv;
var ctx = queries.queryContext({user_id: argv.u});

var conString = process.env.AWS_REDSHIFT_CONN_STRING;

function mkQueryTable(qs) {
    return '( ' + qs + ' )';
}

var allChecks = [
    ic.isPrimaryKey(['messageId'],'messages'),

    ic.isPrimaryKey(['correspondentName'],queries.correspondentNames_rel(ctx)),
    ic.isPrimaryKey(['correspondentId'],queries.correspondentNames_rel(ctx)),

    ic.isPrimaryKey(['emailAddress','correspondentName'],mkQueryTable(queries.fromAddressNamePairs(ctx)),
        {queryName: 'fromAddressNamePairs' }),

    ic.isPrimaryKey(['emailAddress'],mkQueryTable(queries.distinctToOnlyAddrs(ctx)),
        {queryName: 'distinctToOnlyAddrs' }),

    ic.isLowerColumn(['emailAddress'],mkQueryTable(queries.toOnlyNamePairs(ctx)),
        {queryName: 'toOnlyNamePairs'}),

    ic.isPrimaryKey(['userRecipientEmailAddress'],mkQueryTable(tq.tu_UserRecips(ctx)),
        {queryName: 'tu_UserRecips'}),
    ic.isLowerColumn(['userRecipientEmailAddress'],mkQueryTable(tq.tu_UserRecips(ctx)),
        {queryName: 'tu_UserRecips'}),

    ic.isPrimaryKey(['emailAddress','correspondentName'],mkQueryTable(queries.allNamePairs(ctx)),
        {queryName: 'allNamePairs' }),
    ic.isLowerColumn(['emailAddress'],mkQueryTable(queries.allNamePairs(ctx)),
        {queryName: 'allNamePairs' }),

    ic.isPrimaryKey(['emailAddress'], mkQueryTable(tq.tu_BestCorrespondentNames(ctx)),
        {queryName: 'tu_BestCorrespondentNames'}),
    ic.isLowerColumn(['emailAddress'],mkQueryTable(tq.tu_BestCorrespondentNames(ctx)),
        {queryName: 'tu_BestCorrespondentNames'}),

    ic.isPrimaryKey(['emailAddress'], mkQueryTable(tq.tu_CorrespondentEmails(ctx)),
        {queryName: 'tu_CorrespondentEmails'}),
    ic.isLowerColumn(['emailAddress'],mkQueryTable(tq.tu_CorrespondentEmails(ctx)),
        {queryName: 'tu_CorrespondentEmails'}),

    ic.isPrimaryKey(['emailAddress'], queries.correspondentEmails_rel(ctx)),
    ic.isLowerColumn(['emailAddress'], queries.correspondentEmails_rel(ctx))
];

ic.runChecks(conString,allChecks);