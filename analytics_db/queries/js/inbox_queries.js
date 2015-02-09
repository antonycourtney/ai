/*
 * Library of useful SQL queries against Analytic Inbox data warehouse
 */
'use strict';
var _ = require('lodash');
var moment = require('moment');

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
      date(coalesce(received,date)) as dt,
      recipientCorrespondentId as correspondentId,
      recipientCorrespondentName as correspondentName,
      count(*) as messagesSent
  from ${fromUserCIDMessagesRecipients(ctx)}
  group by dt,correspondentId,correspondentName`;

var messagesReceivedCountPerCorrespondentPerDay = (ctx) => `
  select 
      date(coalesce(received,date)) as dt,
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

var correspondentHistoricalRecvSentRatio = (ctx) => `
  select cmx.*,
    (case cmx.messagesSent 
     when 0 then NULL
     else CAST(cmx.messagesReceived AS REAL) / cmx.messagesSent
     end) as recvSentRatio
  from (${correspondentMessagesExchangedSinceDate(ctx,'1980-01-01')}) cmx
  order by recvSentRatio desc`;

var correspondentReceivedFirstLast = (ctx) => `
    select FromCorrespondentId,
           FromCorrespondentName,
           min(coalesce(received,date)) as FirstReceived,
           max(coalesce(received,date)) as LastReceived
    from ${directToUserMessages(ctx)} cm
    group by FromCorrespondentId,FromCorrespondentName`;

var correspondentSentFirstLast = (ctx) => `
    select RecipientCorrespondentID, 
           RecipientCorrespondentName,
           min(coalesce(received,date)) as FirstSent,
           max(coalesce(received,date)) as LastSent
    from ${fromUserCIDMessagesRecipients(ctx)} fu
    group by RecipientCorrespondentID,RecipientCorrespondentName`;

var topCorrespondents = (ctx,qp) => `
  select mx.correspondentName,
         messagesSent,messagesReceived,messagesExchanged,
         case when lmr.lastReceived > lms.lastSent then lmr.lastReceived
              else lms.lastSent
         end as lastContact
  from (${correspondentMessagesExchangedSinceDate(ctx,qp.start_date)}) mx,
    (${correspondentReceivedFirstLast(ctx)}) lmr,
    (${correspondentSentFirstLast(ctx)}) lms
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
select threadId,messageId,received,substring(subject,0,96) as subject
from ${directToUserMessages(ctx)}
where fromCorrespondentName='${qp.correspondent_name}'
order by received desc`;

var directToUserMessagesFromCorrespondentNameGrouped = (ctx,qp) => `
select 
  substring(min(subject),0,96) as subject,
  max(coalesce(received,date)) as received,
  count(*) as messageCount,
  threadId
from ${directToUserMessages(ctx)}
where fromCorrespondentName='${qp.correspondent_name}'
group by threadId
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
 * Group together all the queries used to create the derived tables in an array
 * This can then be used in a query of the form:
 * var queryPromise = pgutils.qpg(conString,pgutils.mkQuerySequence(rebuildDerivedTables(ctx, userRealName, userEmailAddrs)));
 */
var rebuildDerivedTables = (ctx, userRealName, userEmailAddrs) => [
    'vacuum',
    rebuildCorrespondentTables(ctx,userRealName,userEmailAddrs),
    createCIDMessagesView(ctx), 
    createCIDMessagesRecipients(ctx),
    createDirectToUserMessages(ctx,userRealName),
    createFromUserMessagesRecips(ctx,userRealName),
    'vacuum; analyze'
  ];

var checkDerivedTables = (ctx) => `select count(*) from pg_table_def where tablename = '${cidMessages(ctx).toLowerCase()}'`;


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

var dbDateRange = (ctx) => `
  SELECT min(date(received)) AS mindate,
          max(date(received)) AS maxdate
  FROM ${messages_view(ctx)}`;

// final date condition just for testing during dev
// ...but we need it to ensure mxTrailingN has had
// as chance to accumulate enough data!
// TODO: remove or make parametric!
var dbCalendar = (ctx) => `
  SELECT ct.*
  FROM calendar_table ct,
      (${dbDateRange(ctx)}) dbDateRange
  WHERE ct.dt >= dbDateRange.mindate
  AND ct.dt <= dbDateRange.maxdate`;

/*
 * count of messages exchanged per day (and for trailing N days for a few choices of N)
 * for every correspondent on every calendar day
 */
var messagesExchangedFullSeries = (ctx) => `
  WITH
    MessagesExchangedPerDay AS
  (${messagesExchangedCountPerCorrespondentPerDay(ctx)}), 
  
    AllCorrespondents AS
  (SELECT DISTINCT correspondentId,correspondentName
   FROM MessagesExchangedPerDay),
  
    CorrespondentDates AS
  (SELECT ac.*,
         ct.dt
   FROM AllCorrespondents ac,
       (${dbCalendar(ctx)}) ct)
  
  SELECT cd.*,
        COALESCE(mx.messagesExchanged,0) as messagesExchanged,
        avg(CAST(COALESCE(mx.messagesExchanged,0) AS FLOAT)) over
        (partition by cd.correspondentId
         order by cd.dt rows 6 preceding) as mxAverage7,
        sum(COALESCE(mx.messagesExchanged,0)) over
        (partition by cd.correspondentId
         order by cd.dt rows 6 preceding) as mxTrailing7,
        sum(COALESCE(mx.messagesExchanged,0)) over
        (partition by cd.correspondentId
         order by cd.dt rows 29 preceding) as mxTrailing30,
        sum(COALESCE(mx.messagesExchanged,0)) over
        (partition by cd.correspondentId          
         order by cd.dt rows 89 preceding) as mxTrailing90,
        sum(COALESCE(mx.messagesExchanged,0)) over        
        (partition by cd.correspondentId
         order by cd.dt rows 364 preceding) as mxTrailing365
  FROM CorrespondentDates cd left outer join MessagesExchangedPerDay mx ON
    cd.dt = mx.dt AND cd.correspondentId = mx.correspondentId AND cd.correspondentName=mx.correspondentName
  ORDER BY dt desc,correspondentId,mxTrailing90 desc`;

/*
 * FIX FIX FIX
 *
 * This was an attempt to determine a "historical maximum rank", i.e. what was the maximum rank that
 * this correspondent ever had.
 * Unfortunately this suffers from bad initialization transients:  If we look at trailing metrics we 
 * shouldn't really start looking back N days until N days past start of the archive.  And we shouldn't
 * collect these metrics for individual correspondents until the first message exchange...
 *
 * A somewhat simpler approach is historicalRank in maxMXHistorical, which just takes the historical
 * maxMXTrailing90 for all corespondents and ranks those.  This is probably good enough.   
 */

var rankedMXSeries = (ctx) => `
  select fs.dt,fs.correspondentId,fs.correspondentName,fs.messagesExchanged,fs.mxAverage7,fs.mxTrailing7,fs.mxTrailing30,fs.mxTrailing90,fs.mxTrailing365,
      (case when dt < DATE('2006-01-01') then null
       else  
        rank() over
        (partition by dt
         order by mxTrailing90 desc
        ) 
       end) as dailyRank
  from (${messagesExchangedFullSeries(ctx)}) fs`;

/* maximum historical values for mxTrailing7, mxTrailing30 and top (min) historical rank */
var maxMXHistorical = (ctx) => `
  select rms.correspondentId,rms.correspondentName,
  max(rms.mxTrailing7) as maxMXTrailing7,
  max(rms.mxTrailing30) as maxMXTrailing30,
  max(rms.mxTrailing90) as maxMXTrailing90,
  rank() over (order by max(rms.mxTrailing90) desc) as historicalRank,
  min(rms.dailyRank) as topDailyRank
  from (${rankedMXSeries(ctx)}) rms
  group by correspondentId,correspondentName
  order by maxMXTrailing90 desc
  `;


// top ranked correspondents from MX series:
var topRankedMXSeries = (ctx) => `
  WITH RankedFullSeries AS
    (${rankedMXSeries(ctx)}),
  TopCorrespondents AS
  (select DISTINCT correspondentId
   from RankedFullSeries
   where dailyRank < 3
  )
  SELECT rfs.*
  FROM RankedFullSeries rfs,TopCorrespondents tc
  WHERE rfs.correspondentId = tc.correspondentId
  ORDER BY dt,dailyRank,correspondentId`;

// date used for determining current correspondent rank:
var startDate_1y = moment().subtract(1,'years').format("YYYY-MM-DD");

var today_dateStr = moment().format("YYYY-MM-DD");

var topCorrespondents_1y = (ctx) => `
  select *
  from (${topCorrespondents(ctx,{start_date: startDate_1y})}) x
`;

/* simple count of all messages sent and received with a given correspondent */
var epochDate='1980-01-01';

var mxHistorical = (ctx) => `
  select *
  from (${correspondentMessagesExchangedSinceDate(ctx,epochDate)}) mxh
  order by correspondentId`;


/* Note that we use ordinal week numbers (own) because the calendar table uses PGSQL's
 * extract function to get week numbers, which are based on ISO's week numbering, 
 * which breaks the property that date uniquely determines (year,week).
 * Ordinal week numbers are simpler and clearer, if slightly more challenging to eyeball debug.
 */
var messagesExchangedWithCorrespondentPerWeek = (ctx,qp) => `
  WITH RawMPW AS
  (SELECT year,own,
          sum(messagesReceived) as messagesReceived,
          sum(messagesSent) as messagesSent
   FROM (${messagesExchangedWithCorrespondentPerDay(ctx,qp.correspondent_name)}) src,
                    calendar_table c
   WHERE src.dt = c.dt
   GROUP BY year,own),
  StartEndDates AS
  (SELECT min(dt) AS MinDate,
          max(dt) AS MaxDate
   FROM (${messagesExchangedWithCorrespondentPerDay(ctx,qp.correspondent_name)})),
  WeekEndDates AS
  (SELECT ct.year,ct.own,max(dt) AS dt
   FROM calendar_table ct,
                       StartEndDates
   WHERE ct.dt BETWEEN MinDate AND MaxDate
   GROUP BY ct.year,ct.own),
     MPW AS
  (SELECT med.dt,
          COALESCE(RawMPW.messagesReceived,0) as MessagesReceived,
          /*avg(COALESCE(RawMPW.messagesReceived,0)) over
            (order by dt rows between 4 preceding and current row) as avgMessagesReceived, */
          COALESCE(RawMPW.messagesSent,0) as MessagesSent
          /* COALESCE(RawMPW.messagesSent,0)+ COALESCE(RawMPW.messagesReceived,0) as MessagesExchanged */
   FROM WeekEndDates med
   LEFT OUTER JOIN RawMPW ON med.year=RawMPW.year and med.own=RawMPW.own)
SELECT mpw.*
FROM mpw
ORDER BY dt`;

var corrAllStats = (ctx) => `
  WITH mxc AS
      (${correspondentMessagesExchangedSinceDate(ctx,startDate_1y)}),
    mxr AS
      (${correspondentHistoricalRecvSentRatio(ctx)}),
    crfl AS
      (${correspondentReceivedFirstLast(ctx)}),
    csfl AS
      (${correspondentSentFirstLast(ctx)}),
    mmx AS (${maxMXHistorical(ctx)})
  select mxh.correspondentId, 
         mxh.correspondentName,
         mxc.messagesSent as sent_1y,
         mxc.messagesReceived as received_1y,
         mxc.messagesExchanged as exchanged_1y,
         mxh.messagesSent as totalSent, 
         mxh.messagesReceived as totalReceived, 
         mxh.messagesExchanged as totalExchanged,
         mmx.maxMXTrailing30,
         mmx.maxMXTrailing90,
         mmx.historicalRank,
         mmx.topDailyRank,
         mxr.recvSentRatio,
         csfl.firstSent,
         crfl.firstReceived,
         case when csfl.firstSent < crfl.firstReceived then csfl.firstSent
              else crfl.firstReceived
         end as firstContact,
         csfl.lastSent,
         crfl.lastReceived,
         case when crfl.lastReceived > csfl.lastSent then crfl.lastReceived
              else csfl.lastSent
         end as lastContact,
         crfl.lastReceived > csfl.lastSent as owedMail,
         case when crfl.lastReceived > csfl.lastSent 
              then datediff(day,lastReceived,'${today_dateStr}') 
              else 0 
              end as daysOwed          
  from (${correspondentMessagesExchangedSinceDate(ctx,epochDate)}) mxh
        left outer join mxc on mxh.correspondentId = mxc.correspondentId
        left outer join mxr on mxh.correspondentId = mxr.correspondentId
        left outer join crfl on mxh.correspondentId = crfl.FromCorrespondentId
        left outer join csfl on mxh.correspondentId = csfl.RecipientCorrespondentId
        left outer join mmx on mxh.correspondentId = mmx.correspondentId
  order by maxMXTrailing90 desc`;


/*
 * TODO: There's a fair bit of calc that goes in to corrAllStats that we're totally dropping; we should
 * probably trim this to avoid that rather than just hoping that the query optimizer will save us.
 */
var correspondentsOwedMailRaw = (ctx) => `
  select correspondentName,maxMXTrailing90,lastSent,lastReceived,daysOwed
  from
    (${corrAllStats(ctx)}) cas
  where
    owedMail = true
  and daysOwed < 180` 

var correspondentsOwedMail = (ctx) => `
  select correspondentName,lastSent,lastReceived
  from (${correspondentsOwedMailRaw(ctx)}) x
`;

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
module.exports.rebuildDerivedTables = rebuildDerivedTables;
module.exports.checkDerivedTables = checkDerivedTables;
module.exports.messagesExchangedWithCorrespondentPerDay = messagesExchangedWithCorrespondentPerDay;
module.exports.messagesExchangedWithCorrespondentPerWeek = messagesExchangedWithCorrespondentPerWeek;
module.exports.messagesExchangedWithCorrespondentPerMonth = messagesExchangedWithCorrespondentPerMonth;
module.exports.allCorrespondents = allCorrespondents;
module.exports.commaFromRealNames = commaFromRealNames;
module.exports.recipientNamePairs = recipientNamePairs;
module.exports.messages_view = messages_view;
module.exports.recipients_view = messages_view;
module.exports.correspondentNames_rel = correspondentNames_rel;
module.exports.correspondentEmails_rel = correspondentEmails_rel;
module.exports.createBaseViews = createBaseViews;
module.exports.messagesExchangedCountPerCorrespondentPerDay = messagesExchangedCountPerCorrespondentPerDay;
module.exports.dbDateRange = dbDateRange;
module.exports.dbCalendar = dbCalendar;
module.exports.messagesExchangedFullSeries = messagesExchangedFullSeries;
module.exports.topRankedMXSeries = topRankedMXSeries;
module.exports.correspondentHistoricalRecvSentRatio = correspondentHistoricalRecvSentRatio;
module.exports.maxMXHistorical = maxMXHistorical;
module.exports.rankedMXSeries = rankedMXSeries;
module.exports.corrAllStats = corrAllStats;
module.exports.topCorrespondents_1y = topCorrespondents_1y;
module.exports.directToUserMessagesFromCorrespondentNameGrouped = directToUserMessagesFromCorrespondentNameGrouped;
module.exports.mxHistorical = mxHistorical;
module.exports.correspondentsOwedMail = correspondentsOwedMail;
