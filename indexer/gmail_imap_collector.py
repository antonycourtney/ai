#
# An attempt at connecting to GMail via IMAP using OAuth
#

import sys
import httplib2
import certifi
import writers
import readers
import os

import oauth2
import base64
import imaplib
import email
import collections
import re
import types
import json

from apiclient.discovery import build
from apiclient.http import BatchHttpRequest
from oauth2client.client import flow_from_clientsecrets, OAuth2Credentials, OAuth2WebServerFlow
from oauth2client.tools import run
from oauth2client.file import Storage

from simple_throttler import SimpleThrottler
from gmail_extractor import MessageExtractor

from flopsy.flopsy import Connection, Consumer, Publisher

def GenerateOAuth2String(username, access_token, base64_encode=True):
  """Generates an IMAP OAuth2 authentication string.

  See https://developers.google.com/google-apps/gmail/oauth2_overview

  Args:
    username: the username (email address) of the account to authenticate
    access_token: An OAuth2 access token.
    base64_encode: Whether to base64-encode the output.

  Returns:
    The SASL argument for the OAuth2 mechanism.
  """
  auth_string = 'user=%s\1auth=Bearer %s\1\1' % (username, access_token)
  if base64_encode:
    auth_string = base64.b64encode(auth_string)
  return auth_string


def ImapConnect(user, auth_string):
  """Authenticates to IMAP with the given auth_string.

  Prints a debug trace of the attempted IMAP connection.

  Args:
    user: The Gmail username (full email address)
    auth_string: A valid OAuth2 string, as returned by GenerateOAuth2String.
        Must not be base64-encoded, since imaplib does its own base64-encoding.
  """
  imap_conn = imaplib.IMAP4_SSL('imap.gmail.com')
  # imap_conn.debug = 2
  imap_conn.authenticate('XOAUTH2', lambda x: auth_string)
  imap_conn.select('INBOX')
  return imap_conn

def make_header_dict(headers):
    hdict=collections.defaultdict(list)
    for (name,value) in headers:
        hdict[name].append(value)
    return hdict


#
# find dense interval spans from a sorted list of integers:
# Given an ordered list of ints like:
#   [1,5,6,7,8,12,15,16,17,18,20]
# returns
#   [1,(5,8),12,(15,18),20]
def find_interval_spans(inlist,spanAdjacent=False):
    out = []
    if len(inlist)==0:
        return []
    x = inlist[0]
    prev_y = None
    index = 1;
    while index < len(inlist):
        y = inlist[index]
        if prev_y == None:
            if y == x + 1:
                prev_y = y
            else:
                out.append(x)
                x=y
        else:
            if y == prev_y + 1:
                prev_y = y
            else:
                # discontinuity, emit x and prev_y:
                # if spanAdjacent is False, we don't make an
                # interval span out of two adjacent ints:
                if prev_y==x+1 and not spanAdjacent:
                    out.append(x)
                    out.append(prev_y)
                else:
                    out.append((x,prev_y))
                x = y
                prev_y=None
        index += 1
    # deal with remainder:
    if prev_y==None:
        out.append(x)
    else:
        out.append((x,prev_y))

    return out

#
# format the result of find_interval_spans as a string
# Given:
#   [1,(5,8),12,(15,18),20]
# Returns:
#   "1,5:8,12,15:18,20"
def format_interval_spans(spans):
    def format_entry(entry):
        if type(entry)==types.TupleType:
            (lo,hi)=entry
            ret=str(lo) + ":" + str(hi)
        else:
            ret=str(entry)
        return ret

    span_strs = map(format_entry,spans)
    ret = ",".join(span_strs)
    return ret

class CollectorException(Exception):
    pass

class GmailIMAPCollector:

    def __init__(self, args, index_message=None, amqp_connection=None):
 
        # Path to the client_secret.json file downloaded from the Developer Console
        self.CLIENT_SECRET_FILE = 'Credentials/client_secret_native_app.json'

        # Check https://developers.google.com/gmail/api/auth/scopes for all available scopes
        # we're not using gmail API anymore, so let's fix the scopes:
        self.OAUTH_SCOPE = 'https://mail.google.com/ https://www.googleapis.com/auth/plus.me https://www.googleapis.com/auth/plus.profile.emails.read'

        self.BATCH_SIZE = 1000

        self.args = args

        self.usingRabbit = False

        self.amqp_connection = amqp_connection

        if index_message:

            # Extract the user ID to use for this run
            self.args.userID = index_message["user_id"]

            # Use credentials from RabbitMQ index_message
            self.useRabbitCredentials(index_message)

             # Create the service connection
            self.create_imap_service()

        else:
            forceRun = False
            connected = False

            while not(connected):
                try:
                    # Use local credentials
                    self.useLocalCredentials()

                     # Create the service connection
                    self.create_imap_service()
                    connected = True

                except Exception as e:
                    print "Caught exception creating imap service:", e
                    print "Forcing OAuth flow to run to renew credentials:"
                    forceRun=True
                    connected = False
    
        print "Succesfully connected to IMAP server"

    #
    # use credentials from a RabbitMQ index_message to create the service connection
    #
    def useRabbitCredentials(self, index_message):

        self.index_message = index_message;

        # Create an OAuth flow for the credentials using the environment variables
        if os.environ.get('GOOGLE_CLIENT_ID') and os.environ.get('GOOGLE_CLIENT_SECRET'):
            print "Initializing OAuth flow from environment vars"
            flow = OAuth2WebServerFlow(client_id=os.environ.get('GOOGLE_CLIENT_ID'),
                               client_secret=os.environ.get('GOOGLE_CLIENT_SECRET'),
                               scope=self.OAUTH_SCOPE)
        else:
            print "Initializing OAuth flow from client secrets file"
            flow = flow_from_clientsecrets(self.CLIENT_SECRET_FILE, scope=self.OAUTH_SCOPE)

        self.http = httplib2.Http(ca_certs=certifi.where())

        credentials = OAuth2Credentials(index_message["access_token"], flow.client_id, flow.client_secret,
            index_message["refresh_token"], index_message["expires_at"],
            "https://accounts.google.com/o/oauth2/token",  # Make sure we get a token URI - may not be provided by Google OAuth
            "InboxAnalytics")

        self.credentials = credentials

        # Authorize the httplib2.Http object with our credentials
        self.http = credentials.authorize(self.http)

        self.usingRabbit = True


    #
    # use gmail credentals from local dir for when we are running locally
    #
    def useLocalCredentials(self,forceRun=False):

        # Location of the credentials storage file
        STORAGE = Storage('gmail.storage')

        # Start the OAuth flow to retrieve credentials
        flow = flow_from_clientsecrets(self.CLIENT_SECRET_FILE, scope=self.OAUTH_SCOPE)
        self.http = httplib2.Http(ca_certs=certifi.where())

        # Try to retrieve credentials from storage or run the flow to generate them
        credentials = STORAGE.get()
        if credentials is None or credentials.invalid or forceRun:
          credentials = run(flow, STORAGE, http=self.http)

        self.credentials = credentials
        # Authorize the httplib2.Http object with our credentials
        self.http = credentials.authorize(self.http)


    # try to create the IMAP connection:
    def create_imap_service(self):

        # Build the Google plus service to get the user's email address
        self.plus_service = build('plus', 'v1', http=self.http)
        self.user_info = self.plus_service.people().get(userId='me').execute()

        user_email = self.user_info['emails'][0]['value']

        auth_string = GenerateOAuth2String(user_email, self.credentials.access_token, base64_encode=False)

        self.imap_conn = ImapConnect(user_email,auth_string)

    #
    # sync
    #
    # The goal of sync is to bring us up to date. It does this in chunks of BATCH_SIZE,
    # sending progress messages and uploading data between each batch
    #
    # The intent is that sync will be run on a dedicated thread, which may run for a long time (many minutes or hours)
    # while it brings a new user up to date, or may run for just a few seconds when bringing an existing user up to date
    #
    def sync(self):

        #
        # Get a map of all message_ids. We do this once per run as it pulls a lot of data from Google IMAP
        #
        gmail_id_map = self.get_gmail_message_id_map()
        num_total_messages = len(gmail_id_map)
        print "Got all gmail ids. length: ", num_total_messages

        # Read the message Ids we have already processed
        redshiftReader = readers.RedshiftReader(self.args, self.user_info)
        messageIds = redshiftReader.read_message_ids()

        # Get a list of all the missing UIDs
        missing_uids = self.get_missing_uids(gmail_id_map, messageIds)
        num_missing_ids = len(missing_uids)
        print "Number of missing UIDs: ", num_missing_ids

        while (num_missing_ids > 0):

            # Create the writers and MessageExtractor we need
            csvWriter = writers.CSVWriter(self.args, self.user_info)
            extractor = MessageExtractor(csvWriter)

            # Get the batch UIDS - take the last BATCH_SIZE elements from our list
            # Python is kind - if there are fewer elements than BATCH_SIZE, it will do the right thing here
            batch_uids = missing_uids[-self.BATCH_SIZE:] # Grab the last BATCH_SIZE UIDs for this batch
            num_batch_ids = len(batch_uids)

            # Send a progress message before we get going for anyone interested
            self.sendProgressMessage(num_total_messages, num_missing_ids)

            # Get this batch of mails
            self.get_batch(batch_uids, extractor)

            # Calc our stats
            print "\n\nCompleted batch of size: ", num_batch_ids
            pct_complete= (float(num_batch_ids) / num_missing_ids)
            pc_str = '{:.2%}'.format(pct_complete)
            print "Overall progress: downloaded", num_batch_ids, "of", num_missing_ids, "msg headers (", pc_str, ")"

            # Update the missing_uids to remove this batch. We assume we got them all
            missing_uids = missing_uids[:-self.BATCH_SIZE] # Retain the remaining UIDS
            prev_num_missing_ids = num_missing_ids
            num_missing_ids = len(missing_uids) # should be monotonically decreasing, so the while loop will end
            if (num_missing_ids >= prev_num_missing_ids):
                raise CollectorException("num_missing_ids didn't go down as expected")

            # Close the files and upload them
            csvWriter.close_files()
            extractor.msgWriter.upload_to_s3()
            extractor.msgWriter.upload_to_redshift()
            # extractor.msgWriter.cleanup() # We don't clean up right now

        # Send a progress message when we finish
        self.sendProgressMessage(num_total_messages, num_missing_ids)

    def get_batch(self, batch_uids, extractor):

        print "starting batch of size ", len(batch_uids)

        batch_intervals = find_interval_spans(batch_uids)
        batch_fetch_str = format_interval_spans(batch_intervals)

        result, data = self.imap_conn.uid('fetch', batch_fetch_str, '(X-GM-MSGID X-GM-THRID BODY.PEEK[HEADER])')
        if result!="OK":
            raise CollectorException((result,data)) 

        # id_str sample: '12 (X-GM-THRID 1154313475038638396 X-GM-MSGID 1154313475038638396 UID 12 BODY[HEADER] {2852}'
        # no apparent ordering on these attributes, so match independently:
        msg_id_matcher = re.compile('X-GM-MSGID (\d+)');
        thr_id_matcher = re.compile('X-GM-THRID (\d+)');
        uid_matcher = re.compile('UID (\d+)');

        def match_re(re,str):
            match = re.search(str)
            if match == None:
                msg = "Failed to match IMAP result: RE: '" + re + "', target: '" + str + "'"
                raise CollectorException(msg)
            return match.groups()[0]

        for d in data:
            # IMAP is just bizarre, and returns actual results interleaved with strings
            # with a closing paren...
            if type(d)==types.TupleType:
                (id_str,raw_header_str) = d

                gmail_id_dstr = match_re(msg_id_matcher,id_str)
                thr_id_dstr = match_re(thr_id_matcher,id_str)
                uid_dstr = match_re(uid_matcher,id_str)
            
                gmail_id_dec = int(gmail_id_dstr)
                gmail_id = '{0:x}'.format(gmail_id_dec)

                thr_id_dec = int(thr_id_dstr)
                thr_id = '{0:x}'.format(thr_id_dec)

                uid = int(uid_dstr)

                # try to parse the header:
                message_header = email.message_from_string(raw_header_str)
                headers = message_header.items() 
                hdict = make_header_dict(headers)
                # Add Gmail message ID and thread ID to dictionary
                hdict["id"] = gmail_id
                hdict["threadId"] = thr_id

                extractor.extract_imap_hdict(hdict)

    #
    # get_gmail_message_id_map
    #
    # get all GMail message ids via IMAP:
    # returns a dict mapping GMail Message IDs (as hex strings) to IMAP message UIDs
    #
    def get_gmail_message_id_map(self):
        # do a simple search for all messages
        result, data = self.imap_conn.select('[Gmail]/All Mail')
        print "self.imap.select: result: ", result, ", data: ", data
        if result!="OK":
            raise CollectorException((result,data)) 
        result,data = self.imap_conn.uid('search',None,"ALL")
        if result!="OK":
            raise CollectorException((result,data)) 
        print "get_gmail_message_id_map: len(data): ", len(data)
        
        
        msgNums = data[0].split()
        print "got ", len(msgNums), " message numbers"
        print "first msgNum:", msgNums[0]
        print "last msgNum:", msgNums[-1]
        fetchRange = str(msgNums[0]) + ':' + str(msgNums[-1])     

        print "Fetching GMail message IDs for:", fetchRange

        # for testing: just grab the first:
        # fetchRange = '1'

        result, data = self.imap_conn.uid('fetch', fetchRange, '(X-GM-MSGID)')
        if result!="OK":
            raise CollectorException((result,data)) 
        # map from GMail message ID to IMAP UID:
        messageIdMap = {}

        # Use RegExp to pull out message id and UID
        for d in data:
            matcher = re.compile('(\d+) \(X-GM-MSGID (\d+) UID (\d+)\)');
            result = matcher.search(d)
            # print "re matcher result: ", result.groups()
            (msgNum,msgIdStr,uid) = result.groups()
            msgId = int(msgIdStr)
            hexMsgId = '{0:x}'.format(msgId)
            messageIdMap[hexMsgId] = int(uid)

        return messageIdMap

    #
    # get_missing_uids
    # 
    # Figure out which message UIDs we're missing and return this list.
    #
    def get_missing_uids(self, gmail_id_map, messageIds):

        # Get a map of all messages IDs -> UIDs
        # Get the list of missing mail ids, save the full list of IDs and the missing IDs
        gmail_id_set = set(gmail_id_map.keys())
        missing_ids = gmail_id_set - messageIds
        print "IDs in gmail_id_set not in RedShift: ", len(missing_ids)

        # Convert the missing ids to UIDs
        target_uids = set()
        for gmid in missing_ids:
            target_uids.add(gmail_id_map[gmid])

        # Let's extract and sort them:
        uids_list = list(target_uids)
        uids_list.sort()
        return uids_list

    def sendProgressMessage(self, num_total_messages, num_missing_ids):
        
        # We don't send progress messages if we're not using Rabbit
        if self.usingRabbit:
            if self.amqp_connection:

                # Create the message we want to send
                message = {
                    "num_total_messages": num_total_messages,
                    "num_missing": num_missing_ids,
                    "tenant_uid": self.user_info["id"]
                }

                producer = Publisher("", "ia.gmail.progress", self.amqp_connection)
                producer.publish(json.dumps(message))

            else:
                raise CollectorException("No amqp_connection when sending progress message")

    def fetch_one(self,messageId):
        print "fetching message: ", messageId
        mid_int = int(messageId,16)

        search_str = "(X-GM-MSGID " + str(mid_int) + ")"
        print "search string: '" + search_str + "'"
        # First lets try to find its UID:
        result,data = self.imap_conn.uid('search',None,search_str)
        if result!="OK":
            raise CollectorException((result,data)) 
        print "fetch_one: search result: ", data
        if len(data) < 1:
            print "No results found for message ID"
            return
        uid = data[0]
        # To be continued...