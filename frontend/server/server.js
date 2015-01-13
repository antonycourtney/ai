/**
 * node.js server using expressjs
 */
'use strict';

console.log("Starting the node web server");

var port = process.env.PORT || 9000;

// require packages
var express          = require('express');
var app              = express();
var bodyParser       = require('body-parser');
var expressValidator = require('express-validator');
var cookieParser     = require('cookie-parser');
var session          = require('cookie-session')
var flash            = require('connect-flash');
var serverQueries    = require('./server_queries.js');
var auth             = require('./auth.js');
var indexer          = require('./indexer.js');
var sitePages        = require('./site_pages.js');

// Set up the session cookies
app.use(session({
  secret: process.env.SECRET_KEY_BASE || 'this is the glenmistro secret session key',
  cookie: { maxAge: 60 * 60 * 1000 } // 1 hour
}))

// templates using Consolidate.js and Mustache:
// See https://github.com/visionmedia/consolidate.js
var cons = require('consolidate');

// assign the mustache engine to html files:
app.engine('html', cons.mustache);
// set .html as the default extension 
app.set('view engine', 'html');
app.set('views', __dirname + '/views');

// middleware
app.use(cookieParser());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(expressValidator([]));
app.use(flash());

// Set up auth (NB: Important that this is run after the express session setup above)
auth.setup(app);

// Set up indexing
indexer.setup(app);

// For now -- to allow us to serve HTML files from ./public without enumerating a route for each one:

// Make items placed in 'build' dir available at location /public:
app.use("/public", express.static(__dirname + '/../build'));

app.get('/', function(req, res){
  console.log("/, req.flash('info'): ", req.flash('info'), "req.flash('error'): ", req.flash('error'));
  if (req.user) {
    res.redirect('/home');
  } else {
    res.render('index', {
      errorMessages: req.flash('error').join(', '),
      infoMessages: req.flash('info').join(', ')
    });
  }
});

app.get('/home', sitePages.getHomePage);
app.get('/correspondent/:correspondentName', sitePages.getCorrespondentPage);
app.get('/correspondents', sitePages.getAllCorrespondentsPage);
app.get('/correspondentRankings', sitePages.getCorrespondentRankingsPage);

// Logout the user, then redirect to the home page.
app.get('/logout', function(req, res) {
  req.flash('info', 'Logged out');
  console.log("logout, req.flash('info'): ", req.flash('info'), "req.flash('error'): ", req.flash('error'));
  req.logout();
  res.render('index', {
    errorMessages: req.flash('error'),
    infoMessages: req.flash('info')
  });
});

// get requests for some queries:
app.get('/queries/:queryName', serverQueries.getQuery);

// start
app.listen(port);

process.on('uncaughtException', function(err) {
    console.log("Uncaught Exception Handler:");
    console.log("Error object: ", err);
    console.log("Stack: ", err.stack);
    throw err;
});

console.log('Server started on port ' + port);
