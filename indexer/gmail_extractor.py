#!/usr/bin/python
#
#
# gmail_extractor.py -- functions to extract metadata from GMail messages
#
import sys
import collections
import email.utils
import rfc822
import json
import argparse
import time
import os
import writers


# make headers into a dictionary:
def make_header_dict(headers):
    hdict=collections.defaultdict(list)
    for h in headers:
        hdict[h['name']].append(h['value'])
    return hdict

def process_address_headers(rawAddrs):
    ret=[]
    for rawAddr in rawAddrs:
        addrTuples = rfc822.AddressList(rawAddr).addresslist
        addrRecs = [{ 'parsedRealName': parsedRealName[0:127],
                      'parsedEmailAddress': parsedEmailAddress}
                        for (parsedRealName,parsedEmailAddress) in addrTuples ]
        ret += addrRecs
    if len(ret)==0 and len("".join(rawAddrs).strip()) > 0:
        print "Found unparseable address header: ", rawAddrs
    return ret

def parse_time_header(ths):
    try:
        return email.utils.mktime_tz(email.utils.parsedate_tz(ths))
    except:
        return None

def process_time_headers(timeHeaders):
    parsedTimes=map(parse_time_header, timeHeaders)
    if len(parsedTimes) > 0:
        ts=max(parsedTimes)
    else:
        ts=None
    return ts



class MessageExtractor():
    def __init__(self, msgWriter):
        self.msgIds = set()
        self.cacheHitCount=0
        self.numProcessed=0
        self.bulkCount = 0
        self.msgWriter = msgWriter

    #
    # extract metadata and write one line to CSV output:
    #
    def extract_api_message(self,message):
        mid = message['id'] 
        if mid in self.msgIds:
            print "found duplicate message id ", mid
            # cache hit!
            self.cacheHitCount += 1
            if (self.cacheHitCount % 1000)==0:
                print "message id cache hits so far: ", self.cacheHitCount
        else:
            try:
                msgMeta = process_api_message(message)

                mid = msgMeta['messageId']
                if msgMeta['isBulk']:
                    self.bulkCount += 1
                    if (self.bulkCount % 1000)==0:
                        print "Bulk messages dropped: ", self.bulkCount
                else:
                    try:
                        msgFrom = msgMeta['from'][0]['rawAddress']
                    except:
                        msgFrom = '(unknown)'
                    # print "processed message ", mid, " From: ", msgFrom
                    self.msgWriter.writeMessage(msgMeta)

            except Exception as e:
                # We note that we had a problem, but otherwise move on
                # Note that the message ID will be recorded so we won't retry this message
                print "error processing message {0} - exception {1}".format(mid, str(e))

            # add the message ID to the list
            self.msgIds.add(mid)
            self.numProcessed += 1
            if (self.numProcessed % 1000)==0:
                print "messages processed: ", self.numProcessed

    def process_api_message(m):
        mid=m['id']
        try:
            threadId=m['threadId']
            payload=m['payload']
            labelIds=m.get('labelIds',[])
            labelRec=map(lambda lid: {'labelId': lid}, labelIds)
            headers=payload['headers']
            hdict=make_header_dict(headers)
            prec=map(lambda s: s.lower(),hdict['Precedence'])
            # print "precedence: ", prec
            isBulk=('list' in prec) or ('bulk' in prec)
            subjectHeaders=hdict.get('Subject',[None])
            subject=subjectHeaders[0]
            if subject:
                subject=subject[:255]
            fromRec=process_address_headers(hdict['From'])
            toRec=process_address_headers(hdict['To'])
            ccRec=process_address_headers(hdict['Cc'])
            bccRec=process_address_headers(hdict['Bcc'])
            dateTimeStamp=process_time_headers(hdict.get('Date',[]))
            rcvdTimes=map(lambda rs: rs.split(';')[-1], hdict.get('Received',[]))
            rcvdTimeStamp=process_time_headers(rcvdTimes)
            corrEntry = { 'messageId': mid,
                'threadId': threadId,
                'snippet': m['snippet'],
                'sizeEstimate': m['sizeEstimate'],
                'from': fromRec,
                'to': toRec,
                'cc': ccRec,
                'bcc': bccRec,
                'subject': subject,
                'labelIds': labelRec,
                'date': dateTimeStamp,
                'received': rcvdTimeStamp,
                'isBulk': isBulk }
            return corrEntry
        except Exception as e:
            print "Unexpected error processing message id ", mid,":\nError: ", sys.exc_info()[0]
            raise


    def extract_imap_hdict(self,hdict):
        try:
            msgMeta = self.process_imap_hdict(hdict)

            try:
                msgFrom = msgMeta['from'][0]['rawAddress']
            except:
                msgFrom = '(unknown)'
            # print "processed message ", mid, " From: ", msgFrom
            self.msgWriter.writeMessage(msgMeta)

        except Exception as e:
            # We note that we had a problem, but otherwise move on
            # Note that the message ID will be recorded so we won't retry this message
            print "error processing message {0} - exception {1}".format(hdict['id'], str(e))

        # add the message ID to the list
        self.numProcessed += 1
        if (self.numProcessed % 1000)==0:
            print "messages processed: ", self.numProcessed

    def process_imap_hdict(self, hdict):
        mid=hdict['id']
        threadId=hdict['threadId']
        try:
            # payload=m['payload']
            # labelIds=m.get('labelIds',[])
            # labelRec=map(lambda lid: {'labelId': lid}, labelIds)
            # headers=payload['headers']
            # hdict=make_header_dict(headers)
            # prec=map(lambda s: s.lower(),hdict['Precedence'])
            # print "precedence: ", prec
            # isBulk=('list' in prec) or ('bulk' in prec)
            subjectHeaders=hdict.get('Subject',[None])
            subject=subjectHeaders[0]
            if subject:
                subject=subject[:255]
            fromRec=process_address_headers(hdict['From'])
            toRec=process_address_headers(hdict['To'])
            ccRec=process_address_headers(hdict['Cc'])
            bccRec=process_address_headers(hdict['Bcc'])
            dateTimeStamp=process_time_headers(hdict.get('Date',[]))
            rcvdTimes=map(lambda rs: rs.split(';')[-1], hdict.get('Received',[]))
            rcvdTimeStamp=process_time_headers(rcvdTimes)
            corrEntry = { 'messageId': mid,
                'threadId': threadId,
                'snippet': None,
                'sizeEstimate': None,
                'from': fromRec,
                'to': toRec,
                'cc': ccRec,
                'bcc': bccRec,
                'subject': subject,
                'labelIds': None,
                'date': dateTimeStamp,
                'received': rcvdTimeStamp,
                'isBulk': None }
            return corrEntry
        except Exception as e:
            print "*** Unexpected error processing message id ", mid,":\nError: ", sys.exc_info()[0]
            raise


