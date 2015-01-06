import json
import time
import functools
import sys

from gmail_imap_collector import GmailIMAPCollector
from flopsy.flopsy import Connection, Consumer
from etl_parameters import EtlParameters

# Uncomment if debugger needed
# import pdb; pdb.set_trace()

print "Starting Inbox Analytics Backend / ETL"


# Collect the parameters from env vars, config files and command line vars
etl_parameters = EtlParameters()
args = etl_parameters.processArgs()

# Check if we're getting credentials from RabbitMQ or just getting them locally
if args.useRabbit:

    #
    # If Rabbit, this is our callback
    # The callback is actually a partial application of this function (with the first parameter applied)
    #
    def message_callback(amqp_connection, message):
        print 'Received: ' + message.body
        try:
            message_json = json.loads(message.body)

            # Create the Gmail collector using the credentials we got from RabbitMQ
            gc = GmailIMAPCollector(args, message_json, amqp_connection)

            # Collect and upload the mail
            gc.sync()

        except Exception as e:
            print "Got exception trying to process message. Will ack message and move on"
            print "Exception: ", e
            print "Message: ", message

        finally:
            # Acknowledge the message
            consumer.channel.basic_ack(message.delivery_tag)

    # We're using one of the Rabbit mechanisms - CloudAMQP, Rabbit in a Docker container, or Rabbit specified on the command line
    connected = False
    while not(connected):
        try:
            print 'Connecting to RabbitMQ using: [', args.rabbitIP, "] [", args.rabbitUser, "] [", \
                args.rabbitPwd, "] [", args.rabbitVHost, "] [", args.rabbitPort, "]"
            amqp_connection = Connection(args.rabbitIP, args.rabbitUser, args.rabbitPwd, args.rabbitVHost, args.rabbitPort, False)

            print 'Connected, creating consumer and registering callback'
            consumer = Consumer("fanout", "default_routing_key", "ia.gmail.analyse", "etl_queue", True, False, False, amqp_connection)
            consumer.register(functools.partial(message_callback, amqp_connection))
        
        except Exception as e:
            print "Got following exception trying to connect to RabbitMQ, will retry in 5 seconds: " + str(e)
            time.sleep(5)
        
        else:
            connected = True

    print 'Waiting for messages on RabbitMQ'
    consumer.wait()

else:
    if not args.userID:
        print "--userID is required when not rubbing with --useRabbit true"
        sys.exit(1)

    #Create the Gmail collector using local credentials
    gc = GmailIMAPCollector(args)
    # Collect and upload the mail
    gc.sync()

