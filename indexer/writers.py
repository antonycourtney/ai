#
# Writers for various output formats
# Currently only supports CSV
#
import unicodecsv
import datetime
import csv
import boto
import psycopg2
from boto.s3.key import Key
import os

class CSVWriter:

    CSVSchemaOrder = ['messageId','threadId','receivedTS','fromRealName','fromEmailAddress',
                      'subject','snippet','sizeEstimate','dateTS', 'uid', 'user_id', 'createdAt']

    # These two formats are used to create strings of a certain format, and then interpret them when uploaded to the db. So, they need to change in step with each other.
    # datetime_strfmt = "%Y-%m-%dT%H:%M:%S"
    datetime_strfmt = "%Y-%m-%d %H:%M:%S"
    datetime_dbfmt  = "YYYY-MM-DDTHH:MI:SS"

    def __init__(self, args, user_info):

        # We require a minimum of a user id (i.e. tenant specifier)
        if not(user_info and user_info["id"]):
            raise Exception("User info doesn't have an id")

        # Create a timestamp string, specify its format exactly. The database COPY command below depends on this.
        self.timestamp      = datetime.datetime.utcnow().strftime(CSVWriter.datetime_strfmt)

        # Remember the user_info and args
        self.user_info      = user_info
        self.args           = args
        
        # Message storage - may be many of these per tenant in the file system from previous runs
        self.msgsFilePath   = args.outdir + '/' + args.messagesFile + self.tenantifyName() + ".csv"
        self.msgsFile       = open(self.msgsFilePath,'a')
        self.msgsWriter     = unicodecsv.writer(self.msgsFile, encoding='utf-8')
        
        # Recipients - may be many of these per tenant in the file system from previous runs
        self.recipsFilePath = args.outdir + '/' + args.recipientsFile + self.tenantifyName() + ".csv"
        self.recipsFile     = open(self.recipsFilePath,'a')
        self.recipsWriter   = unicodecsv.writer(self.recipsFile, encoding='utf-8')
        

    #
    # Generate a tenant specific suffix for a filename or S3 key
    #
    def tenantifyName(self):
        filename = "_"

        # Start with the basic User ID
        filename += str(self.args.userID)
        
        # Try to add the email address to the end of the name. If this doesn't work, don't worry about it
        try:
            filename += "_" + self.user_info["emails"][0]["value"]
        except:
            pass

        # Append timestamp
        filename += "_" + self.timestamp

        return filename

    # Write a list of recipients to the recipients file
    def writeRecips(self,messageId,recipents,recipType):
        for recip in recipents:
            rowData=[messageId,recip['parsedRealName'],recip['parsedEmailAddress'],recipType, self.user_info["id"], self.args.userID, self.timestamp]
            self.recipsWriter.writerow(rowData)

    # Write the message metadata to the messages file
    def writeMessage(self, msgData):
        msgFrom=msgData['from']
        if len(msgFrom) < 1:
            msgData['fromRealName']=None
            msgData['fromEmailAddress']=None
        else:
            fromRec = msgData['from'][0]
            msgData['fromRealName']=fromRec['parsedRealName']
            msgData['fromEmailAddress']=fromRec['parsedEmailAddress']

        # Now reformat received and date in RedShift Timestamp format:
        receivedTime = msgData.get('received',None)
        if receivedTime:
            receivedDT = datetime.datetime.fromtimestamp(receivedTime)
            msgData['receivedTS'] = receivedDT.strftime(CSVWriter.datetime_strfmt)
        else:
            msgData['receivedTS'] = None
        dateTime = msgData.get('date',None)
        if dateTime:
            dateDT = datetime.datetime.fromtimestamp(dateTime)
            msgData['dateTS'] = dateDT.strftime(CSVWriter.datetime_strfmt)
        else:
            msgData['dateTS'] = None
        msgData['uid'] = self.user_info["id"]
        msgData['user_id'] = self.args.userID
        msgData['createdAt'] = self.timestamp

        rowData = [msgData[k] for k in CSVWriter.CSVSchemaOrder]

        # Write the message itself
        self.msgsWriter.writerow(rowData)

        # Write the recipiencts
        self.writeRecips(msgData['messageId'],msgData['to'],'to')
        self.writeRecips(msgData['messageId'],msgData['cc'],'cc')
        self.writeRecips(msgData['messageId'],msgData['bcc'],'bcc')

    def close_files(self):
        self.msgsFile.close()
        self.recipsFile.close()

    #
    # Upload the data to S3
    def upload_to_s3(self):

        import sys
        def percent_cb(complete, total):
            sys.stdout.write(str(complete) + "/" + str(total))
            if total > 0:
                sys.stdout.write(" (" + str(complete * 100 / total) + "%)\r")
            sys.stdout.flush()

        conn = boto.connect_s3(self.args.awsAccessKey, self.args.awsSecretKey)
        self.bucket = conn.get_bucket(self.args.awsS3Bucket)

        # Generate key names for the messages and recipients
        self.msgsKeyName = "gmail_messages" + self.tenantifyName()
        self.recipsKeyName = "gmail_recipients" + self.tenantifyName()

        #
        # The key name of the data file is based on the user's ID
        #
        k = Key(self.bucket)

        print 'Uploading messages (%s) to Amazon S3 bucket (%s)' % \
           (self.msgsFilePath, self.args.awsS3Bucket)

        k.key = self.msgsKeyName
        k.set_contents_from_filename(self.msgsFilePath,
            cb=percent_cb, num_cb=10)
        print "\ndone"

        print 'Uploading recipients (%s) to Amazon S3 bucket (%s)' % \
           (self.recipsFilePath, self.args.awsS3Bucket)

        k.key = self.recipsKeyName
        k.set_contents_from_filename(self.recipsFilePath,
            cb=percent_cb, num_cb=10)

        print "\ndone"

    def upload_to_redshift(self):

        print "Uploading from S3 bucket to Redshift"


        #
        # We write dates in python's ISO format above, so here we interpret that format
        # An example is: 2014-11-07T01:30:32.251498
        #
        s3key            = 's3://%s/%s' % (self.args.awsS3Bucket, self.msgsKeyName)
        awsCredentials   = 'aws_access_key_id=%s;aws_secret_access_key=%s' % (self.args.awsAccessKey, self.args.awsSecretKey)
        # messages_upload  = "copy messages (messageId, threadId, received, fromRealName, fromEmailAddress, subject, snippet, sizeEstimate, date, uid, user_id, createdAt) \
        #                         from 's3://%(s3bucket)s/%(s3key)s' credentials '%(awsCredentials)s' csv null as '\\000' timeformat as '%(datetime_dbfmt)s';" \
        #                         % {'s3bucket':self.args.awsS3Bucket, 's3key' : self.msgsKeyName, 'awsCredentials': awsCredentials, 'datetime_dbfmt': CSVWriter.datetime_dbfmt}
        # recipients_upload= "copy recipients (messageId, recipientRealName, recipientEmailAddress, recipientType, uid, user_id, createdAt) \
        #                         from 's3://%(s3bucket)s/%(s3key)s' credentials '%(awsCredentials)s' csv null as '\\000' timeformat as '%(datetime_dbfmt)s';" \
        #                         % {'s3bucket':self.args.awsS3Bucket, 's3key' : self.recipsKeyName, 'awsCredentials': awsCredentials, 'datetime_dbfmt': CSVWriter.datetime_dbfmt}
        messages_upload  = "copy messages (messageId, threadId, received, fromRealName, fromEmailAddress, subject, snippet, sizeEstimate, date, uid, user_id, createdAt) \
                                from 's3://%(s3bucket)s/%(s3key)s' credentials '%(awsCredentials)s' csv null as '\\000';" \
                                % {'s3bucket':self.args.awsS3Bucket, 's3key' : self.msgsKeyName, 'awsCredentials': awsCredentials}
        recipients_upload= "copy recipients (messageId, recipientRealName, recipientEmailAddress, recipientType, uid, user_id, createdAt) \
                                from 's3://%(s3bucket)s/%(s3key)s' credentials '%(awsCredentials)s' csv null as '\\000';" \
                                % {'s3bucket':self.args.awsS3Bucket, 's3key' : self.recipsKeyName, 'awsCredentials': awsCredentials}

        # Connect to Redshift
        conn = psycopg2.connect(host = self.args.redshiftInstance, port = self.args.redshiftPort, \
            database = self.args.redshiftDB, user = self.args.redshiftUser, password = self.args.redshiftPwd)

        # Execute the two uploads to messages and recipients
        cur = conn.cursor()
        cur.execute(messages_upload)
        cur.execute(recipients_upload)
        conn.commit()
        print "Upload from S3 to Redshift complete."

        print "Upload complete"

    def cleanup(self):
        print "Cleaning up S3 and local filesystem"
        
        # Delete the S3 blobs
        self.bucket.delete_key(self.msgsKeyName)
        self.bucket.delete_key(self.recipsKeyName)

        # Close and delete the data files
        self.msgsFile.close()
        self.recipsFile.close()
        os.remove(self.msgsFilePath)
        os.remove(self.recipsFilePath)

