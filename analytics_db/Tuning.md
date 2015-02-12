# Notes on tuning RedShift

Let's pick a couple of queries to use for benchmarking:

messagesExchangedCountPerCorrespondentPerDay

Query:
------
select COALESCE(ms.dt,mr.dt) AS dt,
         COALESCE(ms.correspondentId,mr.correspondentId) AS correspondentId,
         COALESCE(ms.correspondentName,mr.correspondentName) AS correspondentName,
         COALESCE(ms.messagesSent,0) AS messagesSent,
         COALESCE(mr.messagesReceived,0) AS messagesReceived,
         (COALESCE(ms.messagesSent,0) + COALESCE(mr.messagesReceived,0)) AS messagesExchanged
  from 
    (
  select 
      date(coalesce(received,date)) as dt,
      recipientCorrespondentId as correspondentId,
      recipientCorrespondentName as correspondentName,
      count(*) as messagesSent
  from FromUserCIDMessagesRecipients_1
  group by dt,correspondentId,correspondentName) ms 
      full outer join 
    (
  select 
      date(coalesce(received,date)) as dt,
      fromCorrespondentId as correspondentId,
      fromCorrespondentName as correspondentName,
      count(*) as messagesReceived
  from DirectToUserMessages_1
  group by dt,correspondentId,correspondentName) mr 
      on ms.dt = mr.dt and ms.correspondentId=mr.correspondentId


Before: Execution time: 2.4s
After: Execution time: 1.42s
=======
rankedAllNamePairs:
-------------------
SELECT anp.*,
        row_number() over (partition BY emailAddress
                     ORDER BY addrNameMessageCount DESC,correspondentName) AS rank
  FROM (
   SELECT *
   FROM (
  SELECT LOWER(m.fromEmailAddress) AS emailAddress,
          COALESCE(NULLIF(m.fromRealName,''),m.fromEmailAddress) AS correspondentName,
          COUNT(*) AS addrNameMessageCount
   FROM (
  SELECT regexp_replace(m.fromRealName,'^([A-Z][a-z]*),[ ]?([A-Z][a-z]*)([ ][A-Z][.]?)?$','\\2\\3 \\1') as fromRealName,
          m.fromEmailAddress
  FROM messages_v_1 m) m
   WHERE LENGTH(m.fromEmailAddress) > 0
   GROUP BY emailAddress,
            correspondentName)
   UNION (
   SELECT rnp.*
   FROM (
   SELECT *
   FROM (
   SELECT DISTINCT rnp.emailAddress
   FROM (
   SELECT LOWER(r.recipientEmailAddress) AS emailAddress,
          COALESCE(NULLIF(r.recipientRealName,''),r.recipientEmailAddress) AS correspondentName,
          COUNT(*) AS addrNameMessageCount
   FROM (
  SELECT regexp_replace(r.recipientRealName,'^([A-Z][a-z]*),[ ]?([A-Z][a-z]*)([ ][A-Z][.]?)?$','\\2\\3 \\1') as recipientRealName,
          r.recipientEmailAddress
  FROM recipients_v_1 r) r
   WHERE LENGTH(r.recipientEmailAddress) > 0
   GROUP BY emailAddress,
            correspondentName) rnp) minus 
    (
   SELECT DISTINCT fanp.emailAddress
   FROM (
  SELECT LOWER(m.fromEmailAddress) AS emailAddress,
          COALESCE(NULLIF(m.fromRealName,''),m.fromEmailAddress) AS correspondentName,
          COUNT(*) AS addrNameMessageCount
   FROM (
  SELECT regexp_replace(m.fromRealName,'^([A-Z][a-z]*),[ ]?([A-Z][a-z]*)([ ][A-Z][.]?)?$','\\2\\3 \\1') as fromRealName,
          m.fromEmailAddress
  FROM messages_v_1 m) m
   WHERE LENGTH(m.fromEmailAddress) > 0
   GROUP BY emailAddress,
            correspondentName) fanp)) toa
   JOIN (
   SELECT LOWER(r.recipientEmailAddress) AS emailAddress,
          COALESCE(NULLIF(r.recipientRealName,''),r.recipientEmailAddress) AS correspondentName,
          COUNT(*) AS addrNameMessageCount
   FROM (
  SELECT regexp_replace(r.recipientRealName,'^([A-Z][a-z]*),[ ]?([A-Z][a-z]*)([ ][A-Z][.]?)?$','\\2\\3 \\1') as recipientRealName,
          r.recipientEmailAddress
  FROM recipients_v_1 r) r
   WHERE LENGTH(r.recipientEmailAddress) > 0
   GROUP BY emailAddress,
            correspondentName) rnp ON toa.emailAddress = rnp.emailAddress)) anp
  ORDER BY anp.emailAddress,rank

Before: Execution time: 6.64s
After: Execution time: 4.15s
=====

Query: tu_MPW 'killian@killianmurphy.com':
----------
WITH RawMPW AS
  (SELECT year,own,
          sum(messagesReceived) as messagesReceived,
          sum(messagesSent) as messagesSent
   FROM (
   SELECT COALESCE(rmc.dt,smc.dt) AS dt,
          COALESCE(rmc.messagesReceived,0) AS messagesReceived,
          COALESCE(smc.messagesSent,0) AS messagesSent
   FROM (
  select 
      date(received) as dt,
      count(*) as messagesReceived
  from DirectToUserMessages_1
  where fromCorrespondentName='killian@killianmurphy.com'
  group by dt) rmc
   FULL OUTER JOIN (
  select 
      date(received) as dt,
      count(*) as messagesSent
  from FromUserCIDMessagesRecipients_1
  where recipientCorrespondentName='killian@killianmurphy.com'
  group by dt) smc ON rmc.dt=smc.dt
   order by dt) src,
                    calendar_table c
   WHERE src.dt = c.dt
   GROUP BY year,own),
  StartEndDates AS
  (SELECT min(dt) AS MinDate,
          max(dt) AS MaxDate
   FROM (
   SELECT COALESCE(rmc.dt,smc.dt) AS dt,
          COALESCE(rmc.messagesReceived,0) AS messagesReceived,
          COALESCE(smc.messagesSent,0) AS messagesSent
   FROM (
  select 
      date(received) as dt,
      count(*) as messagesReceived
  from DirectToUserMessages_1
  where fromCorrespondentName='killian@killianmurphy.com'
  group by dt) rmc
   FULL OUTER JOIN (
  select 
      date(received) as dt,
      count(*) as messagesSent
  from FromUserCIDMessagesRecipients_1
  where recipientCorrespondentName='killian@killianmurphy.com'
  group by dt) smc ON rmc.dt=smc.dt
   order by dt)),
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
ORDER BY dt

Before: Execution time: 2.35s
After: Execution time: 2.01s

In short: Nothing really stellar here.

Changes made:

Added

```
DISTSTYLE KEY
DISTKEY (messageId)
SORTKEY (user_id,"date",messageId);
```
to `CREATE TABLE messages`

and

```
DISTSTYLE KEY
DISTKEY (messageId)
SORTKEY (user_id,recipientEmailAddress);
```

to `CREATE TABLE recipients`.

Also added `ENCODE BYTEDICT` to `recipientType` column in recipients.

Time to start looking at explain plan...

