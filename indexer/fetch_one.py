#
# command-line utility to fetch just one message by messageID and print it to stdout
# 
import json
import time
import functools
import sys

from etl_parameters import EtlParameters
from gmail_imap_collector import GmailIMAPCollector

# Uncomment if debugger needed
# import pdb; pdb.set_trace()

# arg parser:  Enhance with single mandatory arg for message id:
class FetchParameters(EtlParameters):
  def getArgParser(self):
    argParser = EtlParameters.getArgParser(self)
    argParser.add_argument('messageID',metavar='mid',type=str,help='Message ID of message to retrieve, as it appears in Redshift')
    return argParser  

# Collect the parameters from env vars, config files and command line vars
fetch_parameters = FetchParameters()
args = fetch_parameters.processArgs()

# Create the Gmail collector using local credentials
gc = GmailIMAPCollector(args)

msg = gc.fetch_one(args.messageID)

