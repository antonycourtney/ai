#!/usr/bin/python
#
# gmail_collector.py -- Collect Gmail account data, extract metadata and dump it into an S3 bucket
#
import sys
import httplib2
import certifi
import writers
import readers
import os

from apiclient.discovery import build
from apiclient.http import BatchHttpRequest
from oauth2client.client import flow_from_clientsecrets, OAuth2Credentials, OAuth2WebServerFlow
from oauth2client.tools import run
from oauth2client.file import Storage

from simple_throttler import SimpleThrottler
from gmail_extractor import MessageExtractor

class GmailAPICollector:

    def __init__(self, args, message=None):
 
        # Path to the client_secret.json file downloaded from the Developer Console
        self.CLIENT_SECRET_FILE = 'Credentials/client_secret_native_app.json'

        # Check https://developers.google.com/gmail/api/auth/scopes for all available scopes
        self.OAUTH_SCOPE = 'https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/plus.me https://www.googleapis.com/auth/plus.profile.emails.read https://www.googleapis.com/auth/userinfo.profile'

        if message:
            # Use credentials from RabbitMQ message
            self.useRabbitCredentials(message)
        else:
            # Use local credentials
            self.useLocalCredentials()

        # Create the service connection
        self.create_gmail_service()

        # Create the writers and MessageExtractor we need
        self.csvWriter = writers.CSVWriter(args, self.user_info)
        self.extractor = MessageExtractor(self.csvWriter)

        # Read the message Ids we have already processed
        self.redshiftReader = readers.RedshiftReader(args, self.user_info)
        self.messageIds = self.redshiftReader.read_message_ids()


    #
    # use credentials from a RabbitMQ message to create the service connection
    #
    def useRabbitCredentials(self, message):
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

        credentials = OAuth2Credentials(message["access_token"], flow.client_id, flow.client_secret,
            message["refresh_token"], message["expires_at"],
            # message["token_uri"] or "https://accounts.google.com/o/oauth2/token",  # Make sure we get a token URI - may not be provided by Google OAuth
            "https://accounts.google.com/o/oauth2/token",  # Make sure we get a token URI - may not be provided by Google OAuth
            "InboxAnalytics")

        # Authorize the httplib2.Http object with our credentials
        self.http = credentials.authorize(self.http)

    #
    # use gmail credentals from local dir for when we are running locally
    #
    def useLocalCredentials(self):

        # Location of the credentials storage file
        STORAGE = Storage('gmail.storage')

        # Start the OAuth flow to retrieve credentials
        flow = flow_from_clientsecrets(self.CLIENT_SECRET_FILE, scope=self.OAUTH_SCOPE)
        self.http = httplib2.Http(ca_certs=certifi.where())

        # Try to retrieve credentials from storage or run the flow to generate them
        credentials = STORAGE.get()
        if credentials is None or credentials.invalid:
          credentials = run(flow, STORAGE, http=self.http)

        # Authorize the httplib2.Http object with our credentials
        self.http = credentials.authorize(self.http)


    #
    # Create the gmail service from an http connection
    def create_gmail_service(self):
        try:
            # Build the Gmail service from discovery
            self.gmail_service = build('gmail', 'v1', http=self.http)

            # Build the Google plus service to get the user's email address
            self.plus_service = build('plus', 'v1', http=self.http)
            self.user_info = self.plus_service.people().get(userId='me').execute()

        except Exception as e:
            raise

    #
    # perform a full sync against GMail, appending results into the specified
    # messages collection:
    def full_sync( self ):

        global syncHistoryId
        syncHistoryId=None

        # Callback to be called as result of batch read of messages:
        def handle_message_get(request_id,response,exception):
            global syncHistoryId
            if exception is not None:
                print "Got exception during batch message get: ", sys.exc_info()[0]
                raise
            # print "Got response from batched message get: ", response
            message=response
            if syncHistoryId==None:
               syncHistoryId=message['historyId']
            # check again for message id already in collection.
            # can happen due to a retry (for example):
            mid = message['id']

            # Extract the message, write to CSV
            self.extractor.extract_api_message(message)

        print "Collecting Gmail (max 1000 messages)"

        # First some setup
        done = False
        nextPageToken=None
        cacheHitCount=0
        listThrottler = SimpleThrottler(retryBackoff=15)  # throttler for list requests (not batched)
        getThrottler = SimpleThrottler(throttleDelay=11,retryBackoff=15)
        
        while not done:
            # Get the next page of the user's email messages (no maximum # yet - TODO)
            # We use a throttler to ensure we don't overwhelm Google's API server
            listReq = self.gmail_service.users().messages().list(userId='me',pageToken=nextPageToken)
            listRes = listThrottler.execute(listReq)

            # The list of messages comes back, with IDs and estimates of result sizes
            listMsgObjs = listRes['messages']
            print "messages.list returned ", len(listMsgObjs), " message Ids, result size estimate: ", listRes['resultSizeEstimate']
            print "processing list result:"

            # For each message we add to a batch queue to fetch the message data itself
            fetchmids=[]
            batch = BatchHttpRequest(callback=handle_message_get)
            for mobj in listMsgObjs:
                mid = mobj['id']
                if mid in self.messageIds:
                    # cache hit! We don't fetch the message, it's already been read
                    cacheHitCount += 1
                    if (cacheHitCount % 10)==0:
                        print "message id cache hits so far: ", cacheHitCount
                else:
                    # cache miss - fetch the message
                    fetchmids.append(mid)
                    batch.add(self.gmail_service.users().messages().get(userId='me',id=mid))

            # Now let's execute the batch:
            if len(fetchmids) > 0:
                print "Retrieiving ", len(fetchmids), " messages as a batch:"
                getThrottler.execute(batch)
                print "Batch request complete."

            if 'nextPageToken' in listRes:
                nextPageToken=listRes['nextPageToken']
            else:
                done=True

        # Close files before continuing
        self.csvWriter.close_files()

        return done

    def collect_and_upload(self):
        allDone = self.full_sync()
        self.extractor.msgWriter.upload_to_s3()
        self.extractor.msgWriter.upload_to_redshift()
        self.extractor.msgWriter.cleanup()
        return allDone
