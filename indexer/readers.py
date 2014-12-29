import unicodecsv
import psycopg2
from itertools import chain

class RedshiftReader:

    def __init__(self, args, user_info):

        # We require a minimum of a user id (i.e. tenant specifier)
        if not(user_info and user_info["id"]):
            raise Exception("User info doesn't have an id")

        # Remember the user_info and args
        self.user_info     = user_info
        self.args          = args


    def read_message_ids(self):

        messageIds = set()

        download_message_ids = "select messageid from messages where uid = %s"

        # Execute the command against Redshift
        conn = psycopg2.connect(host = self.args.redshiftInstance, port = self.args.redshiftPort, \
            database = self.args.redshiftDB, user = self.args.redshiftUser, password = self.args.redshiftPwd)

        cur = conn.cursor()
        cur.execute(download_message_ids, (self.user_info["id"],))
        messageIds = set(chain.from_iterable(cur.fetchall()))
        conn.close()

        print "read ", len(messageIds), " messages ids from RedShift"
        return messageIds
