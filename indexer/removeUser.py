#
# Remove user
# Remove a user (identified by user_id) from the Redshift and Postgres databases
#

import argparse
import os
import sys
import psycopg2

from user_db import UserDb
from etl_parameters import EtlParameters

#import pdb; pdb.set_trace()

# Collect the parameters from env vars, config files and command line vars
etl_parameters = EtlParameters()
args = etl_parameters.processArgs()

if not args.userID:
    print "--userID <user id> is required"
    sys.exit(1)


# Remove the user from Postgres
user_db = UserDb(args.userDbParams)
user_db.remove_user(args.userID)


def correspondentNames_rel(user_id):
	return "correspondentNames_%(user_id)s" % {'user_id': user_id}

def correspondentEmails_rel(user_id):
	return "correspondentEmails_%(user_id)s" % {'user_id': user_id}

def messages_view(user_id):
	return "messages_v_%(user_id)s" % {'user_id': user_id}

def recipients_view(user_id):
	return "recipients_v_%(user_id)s" % {'user_id': user_id}

def cidMessages(user_id):
	return "CIDMessages_%(user_id)s" % {'user_id': user_id}

def cidMessagesRecipients(user_id):
	return "CIDMessagesRecipients_%(user_id)s" % {'user_id': user_id}

def directToUserMessages(user_id):
	return "DirectToUserMessages_%(user_id)s" % {'user_id': user_id}

def fromUserCIDMessagesRecipients(user_id):
	return "FromUserCIDMessagesRecipients_%(user_id)s" % {'user_id': user_id}

# Remove the user's tables and views from Redshift
remove_user = " \
			begin; \
			drop table if exists %(correspondentNames_rel)s, %(correspondentEmails_rel)s cascade; \
			drop view if exists %(messages_view)s, %(recipients_view)s, %(cidMessages)s, %(cidMessagesRecipients)s, \
								%(directToUserMessages)s, %(fromUserCIDMessagesRecipients)s cascade; \
			delete from messages where user_id=%(user_id)s; \
			delete from recipients where user_id=%(user_id)s; \
			commit; \
			vacuum; analyze; \
       		" \
			% {  'messages_view': messages_view(args.userID),
			    'recipients_view': recipients_view(args.userID),
				'correspondentNames_rel': correspondentNames_rel(args.userID),
				'correspondentEmails_rel': correspondentEmails_rel(args.userID),
				'cidMessages': cidMessages(args.userID),
				'cidMessagesRecipients': cidMessagesRecipients(args.userID),
				'directToUserMessages': directToUserMessages(args.userID),
				'fromUserCIDMessagesRecipients': fromUserCIDMessagesRecipients(args.userID),
				'user_id': args.userID,
				'user_id': args.userID
			}

# Connect to Redshift
conn = psycopg2.connect(host = args.redshiftInstance, port = args.redshiftPort, \
    database = args.redshiftDB, user = args.redshiftUser, password = args.redshiftPwd)

cur = conn.cursor()

# Remove the tables and views
cur.execute(remove_user)
