//
// indexer.js
//
// Various functions to do with indexing
// e.g. /index/status - status of indexing for the logged in user
//      /index/gmail - kick off a job indexing gmail for the logged in user
//

var amqp   = require('amqp');
var models = require('./models.js');
var moment = require('moment');

//
// createAMQPConnection
// Create a connection to the appropriate RabbitMQ server
//
function createAMQPConnection(){

  // These are the default options, stating them explicitly so they can be changed as needed
  var conn_params =     { host: 'localhost'
    , port: 5672
    , login: 'guest'
    , password: 'guest'
    , connectionTimeout: 0
    , authMechanism: 'AMQPLAIN'
    , vhost: '/'
    , noDelay: true
    , ssl: { enabled : false
           }
    };

  // If we're running within Docker, get the hostname and port # from our environment
  if (process.env.MQ_PORT) {
    conn_params.host = process.env.MQ_PORT_5672_TCP_ADDR;
    conn_params.port = process.env.MQ_PORT_5672_TCP_PORT;
  }

  console.log("creating AMQP connection with conn_params:", conn_params);

  // Create and return the connection
  return amqp.createConnection(conn_params, {defaultExchangeName: 'ia.gmail.analyse'});

}

//
// Send a message to index email
//
// The other end is expecting something like this:
//
// { "token":"ya29.pwB6SU_ASMEnf_vqEXokgcF9kD9C5ep9OZA5LlLeqDrttAHG7IajtAx7x9XVnVDbmiDOa-yIXth93A",
// "refresh_token":"1/ifo07IcerE6rJ-T74QOSCYnKEXQR4fgIwppQAjXEOfAMEudVrK5jSpoR30zcRFq6",
// "expires_at":1414055815 }
//
function sendGmailIndexMessage(user) {
  console.log("######## sendGmailIndexMessage starting for user: ", user);

  // Need to get the identity being used
  var id = new models.Identity({provider: 'google', user_id: user.id}).fetch().then(function (identityModel) {

    console.log("######## got identity: ", identityModel);

    var message = {
      "access_token": identityModel["attributes"]["access_token"],
      "refresh_token": identityModel["attributes"]["refresh_token"],
      "expires_at": identityModel["attributes"]["expires_at"],
      "user_id": user.id
    };
    console.log("######## built message: ", message);

    console.log("######## creating amqp_connection");
    var amqp_connection = createAMQPConnection();

    amqp_connection.on('ready', 
      function() {
        console.log("######## connection ready, creating exchange");

        amqp_connection.exchange("ia.gmail.analyse",
          { type: 'fanout'
            ,durable: true
            ,autoDelete: false
          },
          function (exchange) {
            console.log('######## exchange ' + exchange.name + ' is open');
            exchange.publish("", message, {}, function (error) { amqp_connection.disconnect(); });
          }
        );
      }
    );

    amqp_connection.on('error', function(e) {
      console.log("######## sendGmailIndexMessage connection error:", e);
    });

  });

}

//
// Listen for progress messages from the backend of the form:
//
// message = {
//     "num_total_messages": num_total_messages,
//     "num_missing": num_missing_ids,
//     "tenant_uid": self.user_info["id"]
// }
// 
// and update the indexer status in the database. The key fields in the table are:
// 
// messages_indexed: 0, 
// total_messages: 0, 
// status_message: "In Progress", 
// last_indexed: new Date().toISOString();
//

function listenForProgressMessages(){

  console.log("######### listenForProgressMessages: creating connection");
  var amqp_connection = createAMQPConnection();

  amqp_connection.on('ready', 
    function () {
      console.log("######### progress connection ready, creating exchange");
      // amqp_connection.publish('ia.gmail.analyse', message);
      amqp_connection.exchange("ia.gmail.progress",
        { type: 'fanout'
          ,durable: true
          ,autoDelete: false
        },
        function (exchange) {
          console.log('######### progress exchange ' + exchange.name + ' is open, creating queue');
          amqp_connection.queue('', {durable: true, autoDelete: false}, function(queue) {
            console.log('######### progress queue open, binding');
            queue.bind(exchange, '', function() {
              console.log('######### progress queue bound, subscribing');
              queue.subscribe(function (message, headers, deliveryInfo, messageObject) {
                var progress = JSON.parse(message.data.toString('utf-8'));
                console.log('######### progress got a message for uid: ' + progress["tenant_uid"]);
                // Figure out which user we're working with
                var id = new models.Identity({provider: 'google', uid: progress["tenant_uid"]}).fetch().then(function (identity) {
                  if (identity) {
                    gsync = new models.GmailSync({user_id: identity.attributes["user_id"]}).fetch().then(function(gsync){
                      if (!gsync) {
                        // Couldn't find a gsync record for this user - initialize a new one
                        gsync = new models.GmailSync({user_id:  identity.attributes["user_id"]});
                      }
                      // Update the gsync record for this user
                      gsync.attributes.total_messages = progress.num_total_messages;
                      gsync.attributes.messages_indexed = progress.num_total_messages - progress.num_missing;
                      if (gsync.attributes.messages_indexed < gsync.attributes.total_messages) {
                        gsync.attributes.status_message = "Indexing in progress";
                      } else {
                        gsync.attributes.status_message = "Indexing complete";
                      }
                      gsync.attributes.last_indexed = new Date().toISOString();
                      gsync.save();
                    });
                  } else {
                    console.log('######### received progress for unknown tenant_uid: ', progress["tenant_uid"])
                  }
                });
              });
            });
          });
        }
      );
    }
  );
};

function gsync_to_status(gsync) {
  return ({
      totalMessages: gsync.attributes.total_messages,
      messagesIndexed: gsync.attributes.messages_indexed,
      statusMessage: gsync.attributes.status_message,
      lastCompleted: gsync.attributes.last_indexed
    });
}

function refreshIndex() {
  // Find all GmailSyncs which haven't been requested for more than [5 minutes? 1 hour?]
  models.GmailSync
  .query(function(qb) {
    qb.where('last_requested', '<', moment().subtract(5, 'minutes').toISOString())
    .orWhereNull('last_requested')
  })
  .fetchAll({ withRelated: 'user' })
  .then(function(all_gsyncs) {
    all_gsyncs.each(function(gsync) {
      // For each of these, update the last requested time and then request a sync
      gsync.attributes.last_requested = new Date().toISOString();
      gsync.save().then(function(gsync) {
        sendGmailIndexMessage(gsync.related('user'));
      });
    });
  });
}

module.exports.setup = function(app) {

  console.log("index setup");

  // Set up the models
  models.setup(app);

  // GmailSync status
  // Stored in the db
  // indexed by user unique id
  // user_id integer, -- Link back to the users table
  // total_messages integer,
  // messages_indexed integer,
  // last_indexed timestamp with time zone
  //
  app.get('/index/status', function (req,resultHandler) {
    if (req.user && req.user.id) {
      console.log("/index/status for user_id: ", req.user.id)
      var gsync = new models.GmailSync({user_id: req.user.id}).fetch().then(function (gsync) {
        if (gsync) {
          resultHandler.json(gsync_to_status(gsync));
        } else {
          new models.GmailSync(
            { user_id: req.user.id, 
              total_messages: 0,
              messages_indexed: 0,
              status_message: "Initializing",
              last_indexed: null
          }).save().then(function(gsync) {
            resultHandler.json(gsync_to_status(gsync));
          })
        }
      });
    }
  });

  //
  // Send a message to kick off indexing of email
  //
  app.get('/index/gmail', function(req, res) {
    if (req.user) {
      sendGmailIndexMessage(req.user)
      res.redirect('/home')
    } else {
      req.flash('error', 'Please log into Google')
      res.redirect('/');
    }
  });

  listenForProgressMessages();

  // Wake up every minute to refresh our index
  setInterval(refreshIndex, 60000);

};

