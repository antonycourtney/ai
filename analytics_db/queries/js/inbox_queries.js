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

var rawMessagesCount=`
select count(*)
from messages`;

var messagesSentCountPerCorrespondentPerDay = `
  select 
      date(received) as dt,
      recipientCorrespondentId as correspondentId,
      recipientCorrespondentName as correspondentName,
      count(*) as messagesSent
  from FromUserCIDMessagesRecipients
  group by dt,correspondentId,correspondentName`;

var messagesReceivedCountPerCorrespondentPerDay = `
  select 
      date(received) as dt,
      fromCorrespondentId as correspondentId,
      fromCorrespondentName as correspondentName,
      count(*) as messagesReceived
  from DirectToUserMessages
  group by dt,correspondentId,correspondentName`;

var messagesExchangedCountPerCorrespondentPerDay = `
  select COALESCE(ms.dt,mr.dt) AS dt,
         COALESCE(ms.correspondentId,mr.correspondentId) AS correspondentId,
         COALESCE(ms.correspondentName,mr.correspondentName) AS correspondentName,
         COALESCE(ms.messagesSent,0) AS messagesSent,
         COALESCE(mr.messagesReceived,0) AS messagesReceived,
         (COALESCE(ms.messagesSent,0) + COALESCE(mr.messagesReceived,0)) AS messagesExchanged
  from 
    (${messagesSentCountPerCorrespondentPerDay}) ms 
      full outer join 
    (${messagesReceivedCountPerCorrespondentPerDay}) mr 
      on ms.dt = mr.dt and ms.correspondentId=mr.correspondentId`;

var correspondentMessagesExchangedSinceDate = `
  select correspondentId,
         correspondentName,
         sum(messagesSent) as messagesSent,
         sum(messagesReceived) as messagesReceived,
         sum(MessagesExchanged) as messagesExchanged
  from (${messagesExchangedCountPerCorrespondentPerDay})
  where dt >= DATE('2013-01-01')
  group by correspondentId,correspondentName`;

var correspondentLastReceived = `
    select FromCorrespondentId,
           FromCorrespondentName,
           max(received) as LastReceived
    from DirectToUserMessages cm
    group by FromCorrespondentId,FromCorrespondentName`;

var correspondentLastSent = `
    select RecipientCorrespondentID, 
           RecipientCorrespondentName,
           max(received) as LastSent
    from FromUserCIDMessagesRecipients fu
    group by RecipientCorrespondentID,RecipientCorrespondentName`;

var topCorrespondents = `
  select mx.correspondentName,
         messagesSent,messagesReceived,messagesExchanged,
         case when lmr.lastReceived > lms.lastSent then lmr.lastReceived
              else lms.lastSent
         end as lastContact
  from (${correspondentMessagesExchangedSinceDate}) mx,
    (${correspondentLastReceived}) lmr,
    (${correspondentLastSent}) lms
  where messagesSent > 5  
  and mx.correspondentId = lmr.fromCorrespondentId
  and mx.correspondentId = lms.recipientCorrespondentId
  order by messagesExchanged desc
  limit 75`;

var correspondentFromName = (nm) => `
  select cn.correspondentId,cn.correspondentName
  from CorrespondentNames cn
  where cn.correspondentName='${nm}'`

var directToUserMessagesFromCorrespondentName = (qp) => `
select messageId,received,subject
from directtousermessages
where fromCorrespondentName='${qp.correspondent_name}'
order by received desc`;

var allCorrespondents = `
select cnms.correspondentId,correspondentName,emailAddress
from correspondentNames cnms natural join correspondentEmails
order by correspondentName,emailAddress`;

/*
 * Code to build up correspondent tables / views
 */

/* 
 * Note that this has only lower case email addresses but mixed case correspondent names.
 * Used solely for finding the 'best' correspondentName for a given email address
 */
var fromAddressNamePairs = `
  SELECT LOWER(m.fromEmailAddress) AS emailAddress,
          COALESCE(NULLIF(m.fromRealName,''),m.fromEmailAddress) AS correspondentName,
          COUNT(*) AS addrNameMessageCount
   FROM messages m
   WHERE LENGTH(m.fromEmailAddress) > 0
   GROUP BY emailAddress,
            correspondentName`;

/*
 * same property as name pairs:  A given email address will only appear in canonical form,
 * but correspondentName will be mixed case
 */
var recipientNamePairs =`
   SELECT LOWER(r.recipientEmailAddress) AS emailAddress,
          COALESCE(NULLIF(r.recipientRealName,''),r.recipientEmailAddress) AS correspondentName,
          COUNT(*) AS addrNameMessageCount
   FROM recipients r
   WHERE LENGTH(r.recipientEmailAddress) > 0
   GROUP BY emailAddress,
            correspondentName`;

var distinctFromAddrs = `
   SELECT DISTINCT fanp.emailAddress
   FROM (${fromAddressNamePairs}) fanp`;

var distinctToAddrs =`
   SELECT DISTINCT rnp.emailAddress
   FROM (${recipientNamePairs}) rnp`;

var distinctToOnlyAddrs = `
   SELECT *
   FROM (${distinctToAddrs}) minus 
    (${distinctFromAddrs})`;

var toOnlyNamePairs =`
   SELECT rnp.*
   FROM (${distinctToOnlyAddrs}) toa
   JOIN (${recipientNamePairs}) rnp ON toa.emailAddress = rnp.emailAddress`;

var allNamePairs =`
   SELECT *
   FROM (${fromAddressNamePairs})
   UNION (${toOnlyNamePairs})`;

/* N.B.!  Critical to use row_number() rather than rank() here to ensure
 * we get a total order, not a partial order.
 * rank() was assigning the same rank in the case of ties, leading to
 * dups in our correspondent tables.
 * Note: not currently used in constructing correspondent tables
 */
var rankedAllNamePairs =`
  SELECT anp.*,
        row_number() over (partition BY emailAddress
                     ORDER BY addrNameMessageCount DESC,correspondentName) AS rank
  FROM (${allNamePairs}) anp
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
var userRecipients = (addrs) => `
   SELECT LOWER(r.recipientEmailAddress) as userRecipientEmailAddress
   FROM messages m
   JOIN recipients r ON m.messageId = r.messageId
   WHERE m.fromEmailAddress IN (${quotedList(addrs)})
   GROUP BY userRecipientEmailAddress
   ORDER BY userRecipientEmailAddress`;

/* N.B.!  Critical to use row_number() rather than rank() here to ensure
 * we get a total order, not a partial order.
 * rank() was assigning the same rank in the case of ties, leading to
 * dups in our correspondent tables.
 */
var rankedNamePairs = (addrs) => `
  SELECT AllNamePairs.*,
          row_number() over (partition BY emailAddress
                       ORDER BY addrNameMessageCount DESC,correspondentName) AS rank
   FROM (${allNamePairs}) AllNamePairs,
        (${userRecipients(addrs)}) MyRecipients
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

var bestCorrespondentNames = (name,addrs) => `
   ( ${userNameMappings(name,addrs)} )
   union all 
   ( SELECT emailAddress,correspondentName
   FROM (${rankedNamePairs(addrs)}) RankedNamePairs
   WHERE rank=1
   AND emailAddress not in (${quotedList(addrs)})
   ORDER BY emailAddress,
            correspondentName )`;

var distinctNamesList = (name,addrs) => `
    SELECT DISTINCT correspondentName
    FROM (${bestCorrespondentNames(name,addrs)}) bcn`;

// query that will be used to construct the correspondentNames table:
var correspondentNames = (name,addrs) => `
    SELECT correspondentName,
    ROW_NUMBER() OVER (ORDER BY correspondentName) AS correspondentId
    FROM (${distinctNamesList(name,addrs)}) dnl`;

// query that will be used to construct correspondentEmails table
// Note: This must refer to the real correspondentNames table
// since the row IDs assigned to correspondentNames can be
// non-deterministic, since it is only a partial order
var correspondentEmails = (name,addrs) => `
  SELECT bcn.emailAddress,
         cn.correspondentId
  FROM (${bestCorrespondentNames(name,addrs)}) bcn,
       CorrespondentNames cn
  WHERE bcn.correspondentName=cn.correspondentName`;

// generate the SQL to rebuild the correspondent tables:
var rebuildCorrespondentTables = (name,addrs) => `
  drop table if exists correspondentNames cascade;
  drop table if exists correspondentEmails cascade;
  create table correspondentNames as 
  ${correspondentNames(name,addrs)};
  create table correspondentEmails as
  ${correspondentEmails(name,addrs)};
`;


/* view creation code:
 * TODO: should probably factor out the query from the create */
/*
 * create a view on messages table that
 * adds fromCorrespondentId
 * and fromCorrespondentName
 * columns
 */
var createCIDMessagesView=`
  create view CIDMessages as
  SELECT m.*,
      ce.correspondentId as fromCorrespondentId,
      cn.correspondentName as fromCorrespondentName
  FROM
      messages m,
      CorrespondentEmails ce,
      correspondentNames cn
  WHERE LOWER(m.fromEmailAddress)=LOWER(ce.emailAddress)
  AND ce.correspondentId=cn.correspondentId`;


/*
 * Join of CIDMessages view with recipients table
 * and correspondent tables to provide correspondent
 * IDs for sender and all recipients.
 */

var createCIDMessagesRecipients=` 
  create view CIDMessagesRecipients as 
  SELECT cm.*,
      r.recipientRealName,
      r.recipientEmailAddress,
      r.recipientType,
      re.correspondentId as recipientCorrespondentId,
      rn.correspondentName as recipientCorrespondentName
  FROM
      CIDMessages cm,
      recipients r,
      CorrespondentEmails re,
      correspondentNames rn
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
var createDirectToUserMessages = (name) => `
  CREATE VIEW DirectToUserMessages AS 
  WITH ToCorrespondent AS
    (${correspondentFromName(name)}),
              TargetMessageIDs AS
    (SELECT DISTINCT messageId
     FROM CIDMessagesRecipients cmr,
                                ToCorrespondent
     WHERE cmr.recipientCorrespondentId=ToCorrespondent.correspondentId
     AND cmr.fromCorrespondentId<>ToCorrespondent.correspondentId /* skip messages from user */
     ),
              TargetMessages AS
    (SELECT cim.*
     FROM CIDMessages cim,
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
var createFromUserMessagesRecips = (name) => `
  create view FromUserCIDMessagesRecipients as
  WITH UserCorrespondent AS
    (${correspondentFromName(name)}),
    TargetMessagesRecipients AS
  (SELECT cmr.*
   FROM CIDMessagesRecipients cmr,
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

var messagesSentToCorrespondentPerDay = (cnm) => `
  select 
      date(received) as dt,
      count(*) as messagesSent
  from FromUserCIDMessagesRecipients
  where recipientCorrespondentName='${cnm}'
  group by dt`;

var messagesReceivedFromCorrespondentPerDay = (cnm) => `
  select 
      date(received) as dt,
      count(*) as messagesReceived
  from DirectToUserMessages
  where fromCorrespondentName='${cnm}'
  group by dt`;

var messagesExchangedWithCorrespondentPerDay = (cnm) => `
   SELECT COALESCE(rmc.dt,smc.dt) AS dt,
          COALESCE(rmc.messagesReceived,0) AS messagesReceived,
          COALESCE(smc.messagesSent,0) AS messagesSent
   FROM (${messagesReceivedFromCorrespondentPerDay(cnm)}) rmc
   FULL OUTER JOIN (${messagesSentToCorrespondentPerDay(cnm)}) smc ON rmc.dt=smc.dt
   order by dt`;

var messagesExchangedWithCorrespondentPerMonth = (qp) => `
  WITH RawMPM AS
  (SELECT year,month,
          sum(messagesReceived) as messagesReceived,
          sum(messagesSent) as messagesSent
   FROM (${messagesExchangedWithCorrespondentPerDay(qp.correspondent_name)}) src,
                    calendar_table c
   WHERE src.dt = c.dt
   GROUP BY year,month),
  StartEndDates AS
  (SELECT min(dt) AS MinDate,
          max(dt) AS MaxDate
   FROM (${messagesExchangedWithCorrespondentPerDay(qp.correspondent_name)})),
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
module.exports.correspondentEmails = correspondentEmails;
module.exports.rankedNamePairs = rankedNamePairs;
module.exports.distinctNamesList = distinctNamesList;
module.exports.correspondentNames = correspondentNames;
module.exports.rebuildCorrespondentTables = rebuildCorrespondentTables;
module.exports.createCIDMessagesView = createCIDMessagesView;
module.exports.createCIDMessagesRecipients = createCIDMessagesRecipients;
module.exports.createDirectToUserMessages = createDirectToUserMessages;
module.exports.createFromUserMessagesRecips = createFromUserMessagesRecips;
module.exports.messagesExchangedWithCorrespondentPerDay = messagesExchangedWithCorrespondentPerDay;
module.exports.messagesExchangedWithCorrespondentPerMonth = messagesExchangedWithCorrespondentPerMonth;
module.exports.allCorrespondents = allCorrespondents;