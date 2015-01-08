/*
 * Library of useful SQL queries against Analytic Inbox data warehouse
 */
'use strict';
var _ = require('lodash');

/* list of string as a quoted list: */
function quotedList(vals) {
  var qvs = vals.map(function (s) { return "'" + s + "'";});
  return qvs.join(',');
}

// create and return a query context, needed by all queries:
// Almost the identity function now, but we may add more later.
function queryContext(options) {
  return {user_id: options.user_id};
};

var messages_view = (ctx) => `messages_v_${ctx.user_id}`;
var recipients_view = (ctx) => `recipients_v_${ctx.user_id}`;

/*
 * create the base views on the messages and recipients tables, and grant access to ai_frontend
 */
var createBaseViews = (ctx) => `
  create or replace view ${messages_view(ctx)} as select * from messages where user_id=${ctx.user_id};
  create or replace view ${recipients_view(ctx)} as select * from recipients where user_id=${ctx.user_id};
  grant select on ${messages_view(ctx)},${recipients_view(ctx)} to ai_frontend`;

var rawMessagesCount = (ctx) => `
select count(*)
from ${messages_view(ctx)}`;

/*
 * For now we'll construct a correspondentNames and correspondentEmails table
 * per user.
 * But in future we may want to make these into views rather than tables,
 * so we call them _rel, since callers don't care if it's a table or a view.
 */
var correspondentNames_rel = (ctx) => `correspondentNames_${ctx.user_id}`
var correspondentEmails_rel = (ctx) => `correspondentEmails_${ctx.user_id}`

var messagesSentCountPerCorrespondentPerDay = (ctx) => `
  select 
      date(received) as dt,
      recipientCorrespondentId as correspondentId,
      recipientCorrespondentName as correspondentName,
      count(*) as messagesSent
  from ${fromUserCIDMessagesRecipients(ctx)}
  group by dt,correspondentId,correspondentName`;

var messagesReceivedCountPerCorrespondentPerDay = (ctx) => `
  select 
      date(received) as dt,
      fromCorrespondentId as correspondentId,
      fromCorrespondentName as correspondentName,
      count(*) as messagesReceived
  from ${directToUserMessages(ctx)}
  group by dt,correspondentId,correspondentName`;

var messagesExchangedCountPerCorrespondentPerDay = (ctx) => `
  select COALESCE(ms.dt,mr.dt) AS dt,
         COALESCE(ms.correspondentId,mr.correspondentId) AS correspondentId,
         COALESCE(ms.correspondentName,mr.correspondentName) AS correspondentName,
         COALESCE(ms.messagesSent,0) AS messagesSent,
         COALESCE(mr.messagesReceived,0) AS messagesReceived,
         (COALESCE(ms.messagesSent,0) + COALESCE(mr.messagesReceived,0)) AS messagesExchanged
  from 
    (${messagesSentCountPerCorrespondentPerDay(ctx)}) ms 
      full outer join 
    (${messagesReceivedCountPerCorrespondentPerDay(ctx)}) mr 
      on ms.dt = mr.dt and ms.correspondentId=mr.correspondentId`;

var correspondentMessagesExchangedSinceDate = (ctx,startDate) => `
  select correspondentId,
         correspondentName,
         sum(messagesSent) as messagesSent,
         sum(messagesReceived) as messagesReceived,
         sum(MessagesExchanged) as messagesExchanged
  from (${messagesExchangedCountPerCorrespondentPerDay(ctx)})
  where dt >= DATE('${startDate}')
  group by correspondentId,correspondentName`;

var correspondentLastReceived = (ctx) => `
    select FromCorrespondentId,
           FromCorrespondentName,
           max(received) as LastReceived
    from ${directToUserMessages(ctx)} cm
    group by FromCorrespondentId,FromCorrespondentName`;

var correspondentLastSent = (ctx) => `
    select RecipientCorrespondentID, 
           RecipientCorrespondentName,
           max(received) as LastSent
    from ${fromUserCIDMessagesRecipients(ctx)} fu
    group by RecipientCorrespondentID,RecipientCorrespondentName`;

var topCorrespondents = (ctx,qp) => `
  select mx.correspondentName,
         messagesSent,messagesReceived,messagesExchanged,
         case when lmr.lastReceived > lms.lastSent then lmr.lastReceived
              else lms.lastSent
         end as lastContact
  from (${correspondentMessagesExchangedSinceDate(ctx,qp.start_date)}) mx,
    (${correspondentLastReceived(ctx)}) lmr,
    (${correspondentLastSent(ctx)}) lms
  where messagesSent > 5  
  and mx.correspondentId = lmr.fromCorrespondentId
  and mx.correspondentId = lms.recipientCorrespondentId
  order by messagesExchanged desc
  limit 75`;

var correspondentFromName = (ctx,nm) => `
  select cn.correspondentId,cn.correspondentName
  from ${correspondentNames_rel(ctx)} cn
  where cn.correspondentName='${nm}'`

var directToUserMessagesFromCorrespondentName = (ctx,qp) => `
select messageId,received,substring(subject,0,96) as subject
from ${directToUserMessages(ctx)}
where fromCorrespondentName='${qp.correspondent_name}'
order by received desc`;

var allCorrespondents = (ctx) => `
select cnms.correspondentId,correspondentName,emailAddress
from ${correspondentNames_rel(ctx)} cnms natural join 
    ${correspondentEmails_rel(ctx)}
order by correspondentName,emailAddress`;

/*
 * Code to build up correspondent tables / views
 */

var lastFirstRE = '^([A-Z][a-z]*)\,[ ]?([A-Z][a-z]*)([ ][A-Z][\.]?)?$';

var commaFromRealNames = (ctx) => `
  SELECT DISTINCT m.fromRealName rnm,
  regexp_count(rnm,'${lastFirstRE}') as cnt,
  regexp_replace(rnm,'${lastFirstRE}','\\\\2\\\\3 \\\\1') as rep
  FROM ${messages_view(ctx)} m
  WHERE strpos(rnm,',') > 0
  ORDER BY rnm`;

/* real name, email address pairs after normalizing real names with
 * the lastFirstRE
 */
var normFromPairs = (ctx) => `
  SELECT regexp_replace(m.fromRealName,'${lastFirstRE}','\\\\2\\\\3 \\\\1') as fromRealName,
          m.fromEmailAddress
  FROM ${messages_view(ctx)} m`;

/* same things, but for recipient name,address pairs: */
var normRecipPairs = (ctx) => `
  SELECT regexp_replace(r.recipientRealName,'${lastFirstRE}','\\\\2\\\\3 \\\\1') as recipientRealName,
          r.recipientEmailAddress
  FROM ${recipients_view(ctx)} r`;

/* 
 * Note that this has only lower case email addresses but mixed case correspondent names.
 * Used solely for finding the 'best' correspondentName for a given email address
 */
// distinct (emailAddress,realName) pairs appearing in From: line, with count of occurrences
var fromAddressNamePairs = (ctx) => `
  SELECT LOWER(m.fromEmailAddress) AS emailAddress,
          COALESCE(NULLIF(m.fromRealName,''),m.fromEmailAddress) AS correspondentName,
          COUNT(*) AS addrNameMessageCount
   FROM (${normFromPairs(ctx)}) m
   WHERE LENGTH(m.fromEmailAddress) > 0
   GROUP BY emailAddress,
            correspondentName`;

/*
 * same property as name pairs:  A given email address will only appear in canonical form,
 * but correspondentName will be mixed case
 */
/* Like fromAddressNamePairs, but over all recipients. Includes count of occurrences */
var recipientNamePairs = (ctx) => `
   SELECT LOWER(r.recipientEmailAddress) AS emailAddress,
          COALESCE(NULLIF(r.recipientRealName,''),r.recipientEmailAddress) AS correspondentName,
          COUNT(*) AS addrNameMessageCount
   FROM (${normRecipPairs(ctx)}) r
   WHERE LENGTH(r.recipientEmailAddress) > 0
   GROUP BY emailAddress,
            correspondentName`;

/* set of distinct email addresses occuring on From: line */
var distinctFromAddrs = (ctx) => `
   SELECT DISTINCT fanp.emailAddress
   FROM (${fromAddressNamePairs(ctx)}) fanp`;

/* set of distinct email addresses appearing as recipient */
var distinctToAddrs = (ctx) => `
   SELECT DISTINCT rnp.emailAddress
   FROM (${recipientNamePairs(ctx)}) rnp`;

/* email addresses that appear only as recipients, never as sender */
var distinctToOnlyAddrs = (ctx) => `
   SELECT *
   FROM (${distinctToAddrs(ctx)}) minus 
    (${distinctFromAddrs(ctx)})`;

/* name pairs of recipients that appear only as recipients */
var toOnlyNamePairs = (ctx) => `
   SELECT rnp.*
   FROM (${distinctToOnlyAddrs(ctx)}) toa
   JOIN (${recipientNamePairs(ctx)}) rnp ON toa.emailAddress = rnp.emailAddress`;

/* combine name pairs appearing in From with those appearing only as recipients.
 *
 * Because we previously removed FromAddrs from toOnly name pairs, we ensure
 * that each pair is distinct in this set, and we (deliberately) favor
 * real names as they appear in From: over their appearance as recipients
 */
var allNamePairs = (ctx) => `
   SELECT *
   FROM (${fromAddressNamePairs(ctx)})
   UNION (${toOnlyNamePairs(ctx)})`;

/* N.B.!  Critical to use row_number() rather than rank() here to ensure
 * we get a total order, not a partial order.
 * rank() was assigning the same rank in the case of ties, leading to
 * dups in our correspondent tables.
 * Note: not currently used in constructing correspondent tables
 */
var rankedAllNamePairs = (ctx) => `
  SELECT anp.*,
        row_number() over (partition BY emailAddress
                     ORDER BY addrNameMessageCount DESC,correspondentName) AS rank
  FROM (${allNamePairs(ctx)}) anp
  ORDER BY anp.emailAddress,rank`;

/* 
 * Achtung!  WARNING!
 * This was originally just:
 *
 * SELECT LOWER(r.recipientEmailAddress) AS recipientEmailAddress
 * ...
 * GROUP BY recipientEmailAddress
 *
 * Unfortunately it appears SQL prefers the column name from the subquery in such cases,
 * which caused duplicates in the userRecipients query.
 * Now fixed by grouping on the explicit LOWER(...) version.
 */
 /* email addresses (in canonical form) of recipients of messages
  * from any of a set of email addresses
  */
var userRecipients = (ctx,addrs) => `
   SELECT LOWER(r.recipientEmailAddress) as userRecipientEmailAddress
   FROM ${messages_view(ctx)} m
   JOIN ${recipients_view(ctx)} r ON m.messageId = r.messageId
   WHERE m.fromEmailAddress IN (${quotedList(addrs)})
   GROUP BY userRecipientEmailAddress
   ORDER BY userRecipientEmailAddress`;

/* N.B.!  Critical to use row_number() rather than rank() here to ensure
 * we get a total order, not a partial order.
 * rank() was assigning the same rank in the case of ties, leading to
 * dups in our correspondent tables.
 */
var rankedNamePairs = (ctx,addrs) => `
  SELECT AllNamePairs.*,
          row_number() over (partition BY emailAddress
                       ORDER BY addrNameMessageCount DESC,correspondentName) AS rank
   FROM (${allNamePairs(ctx)}) AllNamePairs,
        (${userRecipients(ctx,addrs)}) MyRecipients
   WHERE AllNamePairs.emailAddress=MyRecipients.userRecipientEmailAddress
   ORDER BY AllNamePairs.emailAddress, rank`;

/* query to generate correspondent name, email address pairs for user real and email addrs: */
/* A bit horrible because RedShift doesn't support VALUES */
var userNameMappings = function (name,addrs) {
  var mkRow = (ea) => `select '${ea}' as emailAddress,'${name}' as correspondentName`;

  debugger;
  var rows = addrs.map(mkRow);

  var litTable = rows.join('\nunion all\n');

  return litTable;
}

var bestCorrespondentNames = (ctx,name,addrs) => `
   ( ${userNameMappings(name,addrs)} )
   union all 
   ( SELECT emailAddress,correspondentName
   FROM (${rankedNamePairs(ctx,addrs)}) RankedNamePairs
   WHERE rank=1
   AND emailAddress not in (${quotedList(addrs)})
   ORDER BY emailAddress,
            correspondentName )`;

var distinctNamesList = (ctx,name,addrs) => `
    SELECT DISTINCT correspondentName
    FROM (${bestCorrespondentNames(ctx,name,addrs)}) bcn`;

// query that will be used to construct the correspondentNames table:
// We give this this explicitly 'Query' suffix to distinguish it from the corresponding table/view
var correspondentNamesQuery = (ctx,name,addrs) => `
    SELECT correspondentName,
    ROW_NUMBER() OVER (ORDER BY correspondentName) AS correspondentId
    FROM (${distinctNamesList(ctx,name,addrs)}) dnl`;

// query that will be used to construct correspondentEmails table
// Note: This must refer to the real correspondentNames table
// since the row IDs assigned to correspondentNames can be
// non-deterministic, since it is only a partial order
var correspondentEmailsQuery = (ctx,name,addrs) => `
  SELECT bcn.emailAddress,
         cn.correspondentId
  FROM (${bestCorrespondentNames(ctx,name,addrs)}) bcn,
       ${correspondentNames_rel(ctx)} cn
  WHERE bcn.correspondentName=cn.correspondentName`;

// generate the SQL to rebuild the correspondent tables:
var rebuildCorrespondentTables = (ctx,name,addrs) => `
  drop table if exists ${correspondentNames_rel(ctx)} cascade;
  drop table if exists ${correspondentEmails_rel(ctx)} cascade;
  create table ${correspondentNames_rel(ctx)} as 
  ${correspondentNamesQuery(ctx,name,addrs)};
  create table ${correspondentEmails_rel(ctx)} as
  ${correspondentEmailsQuery(ctx,name,addrs)};
`;


/* view creation code:
 * TODO: should probably factor out the query from the create */
/*
 * create a view on messages table that
 * adds fromCorrespondentId
 * and fromCorrespondentName
 * columns
 */
var cidMessages = (ctx) => `CIDMessages_${ctx.user_id}`;

var createCIDMessagesView = (ctx) => `
  create or replace view ${cidMessages(ctx)} as
  SELECT m.*,
      ce.correspondentId as fromCorrespondentId,
      cn.correspondentName as fromCorrespondentName
  FROM
      ${messages_view(ctx)} m,
      ${correspondentEmails_rel(ctx)} ce,
      ${correspondentNames_rel(ctx)} cn
  WHERE LOWER(m.fromEmailAddress)=LOWER(ce.emailAddress)
  AND ce.correspondentId=cn.correspondentId`;

/*
 * Join of CIDMessages view with recipients table
 * and correspondent tables to provide correspondent
 * IDs for sender and all recipients.
 */

var cidMessagesRecipients = (ctx) => `CIDMessagesRecipients_${ctx.user_id}`;

var createCIDMessagesRecipients = (ctx) => ` 
  create or replace view ${cidMessagesRecipients(ctx)} as 
  SELECT cm.*,
      r.recipientRealName,
      r.recipientEmailAddress,
      r.recipientType,
      re.correspondentId as recipientCorrespondentId,
      rn.correspondentName as recipientCorrespondentName
  FROM
      ${cidMessages(ctx)} cm,
      ${recipients_view(ctx)} r,
      ${correspondentEmails_rel(ctx)} re,
      ${correspondentNames_rel(ctx)} rn
  WHERE cm.messageId=r.messageId
  AND LOWER(r.recipientEmailAddress)=LOWER(re.emailAddress)
  AND re.correspondentId=rn.correspondentId`

/*
 * create a view of CIDMessages that were sent directly to user
 * (user appears explicitly in To: or Cc: of message)
 * from correspondents that are not the user.
 * Eliminates some messages from correspondents to mailing lists
 * that both happen to be on.
 */
var directToUserMessages = (ctx) => `DirectToUserMessages_${ctx.user_id}`

var createDirectToUserMessages = (ctx,name) => `
  CREATE VIEW ${directToUserMessages(ctx)} AS 
  WITH ToCorrespondent AS
    (${correspondentFromName(ctx,name)}),
              TargetMessageIDs AS
    (SELECT DISTINCT messageId
     FROM ${cidMessagesRecipients(ctx)} cmr,
                                ToCorrespondent
     WHERE cmr.recipientCorrespondentId=ToCorrespondent.correspondentId
     AND cmr.fromCorrespondentId<>ToCorrespondent.correspondentId /* skip messages from user */
     ),
              TargetMessages AS
    (SELECT cim.*
     FROM ${cidMessages(ctx)} cim,
                      TargetMessageIDs
     WHERE cim.messageId=TargetMessageIDs.messageId)
  SELECT *
  FROM TargetMessages`;

/*
 * create a view of CIDMessagesRecipients restricted to messages
 * from the user, and eliminates rows where the user
 * is shown as a recipient.
 *
 * We expose CIDMessagesRecipients rather than CIDMessages because
 * the correspondent IDs of the recipient will almost always be of interest.
 */
var fromUserCIDMessagesRecipients = (ctx) => `FromUserCIDMessagesRecipients_${ctx.user_id}`

var createFromUserMessagesRecips = (ctx,name) => `
  create view ${fromUserCIDMessagesRecipients(ctx)} as
  WITH UserCorrespondent AS
    (${correspondentFromName(ctx,name)}),
    TargetMessagesRecipients AS
  (SELECT cmr.*
   FROM ${cidMessagesRecipients(ctx)} cmr,
                            UserCorrespondent
   WHERE cmr.fromCorrespondentId=UserCorrespondent.correspondentId
   AND cmr.recipientCorrespondentId<>UserCorrespondent.correspondentId
  )
  select *
  from TargetMessagesRecipients
  order by received desc`;

/*
 * Number of messages per week sent and received with a given
 * correspondent.
 * Based on MessagesFromToCorrespondent, aggregated into
 * weekly rates.
 */

var messagesSentToCorrespondentPerDay = (ctx,cnm) => `
  select 
      date(received) as dt,
      count(*) as messagesSent
  from ${fromUserCIDMessagesRecipients(ctx)}
  where recipientCorrespondentName='${cnm}'
  group by dt`;

var messagesReceivedFromCorrespondentPerDay = (ctx,cnm) => `
  select 
      date(received) as dt,
      count(*) as messagesReceived
  from ${directToUserMessages(ctx)}
  where fromCorrespondentName='${cnm}'
  group by dt`;

var messagesExchangedWithCorrespondentPerDay = (ctx,cnm) => `
   SELECT COALESCE(rmc.dt,smc.dt) AS dt,
          COALESCE(rmc.messagesReceived,0) AS messagesReceived,
          COALESCE(smc.messagesSent,0) AS messagesSent
   FROM (${messagesReceivedFromCorrespondentPerDay(ctx,cnm)}) rmc
   FULL OUTER JOIN (${messagesSentToCorrespondentPerDay(ctx,cnm)}) smc ON rmc.dt=smc.dt
   order by dt`;

var messagesExchangedWithCorrespondentPerMonth = (ctx,qp) => `
  WITH RawMPM AS
  (SELECT year,month,
          sum(messagesReceived) as messagesReceived,
          sum(messagesSent) as messagesSent
   FROM (${messagesExchangedWithCorrespondentPerDay(ctx,qp.correspondent_name)}) src,
                    calendar_table c
   WHERE src.dt = c.dt
   GROUP BY year,month),
  StartEndDates AS
  (SELECT min(dt) AS MinDate,
          max(dt) AS MaxDate
   FROM (${messagesExchangedWithCorrespondentPerDay(ctx,qp.correspondent_name)})),
  MonthEndDates AS
  (SELECT ct.year,ct.month,max(dt) AS dt
   FROM calendar_table ct,
                       StartEndDates
   WHERE ct.dt BETWEEN MinDate AND MaxDate
   GROUP BY ct.year,ct.month),
     MPM AS
  (SELECT med.dt,
          COALESCE(RawMPM.messagesReceived,0) as MessagesReceived,
          /*avg(COALESCE(RawMPW.messagesReceived,0)) over
            (order by dt rows between 4 preceding and current row) as avgMessagesReceived, */
          COALESCE(RawMPM.messagesSent,0) as MessagesSent
          /* COALESCE(RawMPW.messagesSent,0)+ COALESCE(RawMPW.messagesReceived,0) as MessagesExchanged */
   FROM MonthEndDates med
   LEFT OUTER JOIN RawMPM ON med.year=RawMPM.year and med.month=RawMPM.month)
SELECT mpm.*
FROM mpm
ORDER BY dt`;

/* TODO: messagesExchangedWithCorrespondentPerWeek */
  /* At this point we have daily counts, now just aggregate to get weekly numbers: */
  /* Note that we use ordinal week numbers (own) because the calendar table uses PGSQL's
   * extract function to get week numbers, which are based on ISO's week numbering, 
   * which breaks the property that date uniquely determines (year,week).
   * Ordinal week numbers are simpler and clearer, if slightly more challenging to eyeball debug.
   */

module.exports.queryContext = queryContext;
module.exports.fromAddressNamePairs = fromAddressNamePairs;
module.exports.rawMessagesCount = rawMessagesCount;
module.exports.topCorrespondents = topCorrespondents;
module.exports.correspondentFromName = correspondentFromName;
module.exports.directToUserMessagesFromCorrespondentName = directToUserMessagesFromCorrespondentName;
module.exports.fromAddressNamePairs = fromAddressNamePairs;
module.exports.distinctToOnlyAddrs = distinctToOnlyAddrs;
module.exports.toOnlyNamePairs = toOnlyNamePairs;
module.exports.allNamePairs = allNamePairs;
module.exports.rankedAllNamePairs = rankedAllNamePairs;
module.exports.userRecipients = userRecipients;
module.exports.bestCorrespondentNames = bestCorrespondentNames;
module.exports.correspondentEmailsQuery = correspondentEmailsQuery;
module.exports.rankedNamePairs = rankedNamePairs;
module.exports.distinctNamesList = distinctNamesList;
module.exports.correspondentNamesQuery = correspondentNamesQuery;
module.exports.rebuildCorrespondentTables = rebuildCorrespondentTables;
module.exports.createCIDMessagesView = createCIDMessagesView;
module.exports.createCIDMessagesRecipients = createCIDMessagesRecipients;
module.exports.createDirectToUserMessages = createDirectToUserMessages;
module.exports.createFromUserMessagesRecips = createFromUserMessagesRecips;
module.exports.messagesExchangedWithCorrespondentPerDay = messagesExchangedWithCorrespondentPerDay;
module.exports.messagesExchangedWithCorrespondentPerMonth = messagesExchangedWithCorrespondentPerMonth;
module.exports.allCorrespondents = allCorrespondents;
module.exports.commaFromRealNames = commaFromRealNames;
module.exports.recipientNamePairs = recipientNamePairs;
module.exports.messages_view = messages_view;
module.exports.recipients_view = messages_view;
module.exports.correspondentNames_rel = correspondentNames_rel;
module.exports.correspondentEmails_rel = correspondentEmails_rel;
module.exports.createBaseViews = createBaseViews;