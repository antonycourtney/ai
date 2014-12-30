var moment           = require('moment');
var models           = require('./models.js');

// google OAuth stuff:
var google           = require('googleapis');
var OAuth2Client     = google.auth.OAuth2;
var plus             = google.plus('v1');

var passport         = require('passport');
var GoogleStrategy   = require('passport-google-oauth').OAuth2Strategy;

// Client ID and client secret are available at
// https://code.google.com/apis/console
var GOOGLE_CLIENT_ID     = process.env.GOOGLE_CLIENT_ID;
var GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
var GOOGLE_REDIRECT_URL  = '/auth/google_oauth2/callback';

// Google OAuth2
passport.use(new GoogleStrategy({
    clientID: GOOGLE_CLIENT_ID,
    clientSecret: GOOGLE_CLIENT_SECRET,
    callbackURL: GOOGLE_REDIRECT_URL
  },
  function(accessToken, refreshToken, params, profile, done) {
    console.log("profile: ", profile, "\n\naccessToken: ", accessToken, "\n\nrefreshToken: ", refreshToken, "\n\nparams: ", params)
    console.log("expires_in ", params.expires_in, " seconds");
    var expires_at = moment().add(params.expires_in, "seconds");
    new models.Identity({provider: 'google', uid: profile.id}).fetch().then(function (identityModel) {
      if (identityModel) {
        console.log("found identity in db: ", identityModel, identityModel.user_id);
        identityModel.set({access_token: accessToken, refresh_token: refreshToken, expires_at: expires_at}).save().then(function (identityModel) {
          var userRel = identityModel.related('user').fetch().then(function(userModel){
            console.log("Loaded userModel, got: ", userModel);
            return done(null, userModel);
          });
        });
      } else {
        console.log("No identity found in db, creating...\n");
        console.log("expires_at: ", expires_at.format())
        var user = new models.User({real_name: profile.displayName }).save().then(function (userModel) {
          var identity = new models.Identity({user_id: userModel.id, provider: 'google', uid: profile.id, access_token: accessToken, refresh_token: refreshToken, expires_at: expires_at});
          identity.save().then(function (identityModel) {
            return done(null,userModel);
          });
        });
      }
    });
  }
));

passport.serializeUser(function(user, done) {
  done(null, user.id);
});

passport.deserializeUser(function(id, done) {
  new models.User({id: id}).fetch().then(function(userModel) {
    done(null, userModel);
  });
});


module.exports.setup = function(app) {

  console.log("auth setup");

  app.use(passport.initialize());
  app.use(passport.session());

	// Redirect the user to Google for authentication.  When complete, Google
	// will redirect the user back to the application at
	//     /auth/google_oauth2/callback
	app.get('/auth/google_oauth2', 
		passport.authenticate('google', 
			{scope: ['email',
	                 'profile',
	                 'https://www.googleapis.com/auth/gmail.readonly',
	                 'https://mail.google.com/'],
	       	accessType: 'offline',
	       	approvalPrompt: 'force'
	     	})
	);

	// Google will redirect the user to this URL after authentication.  Finish
	// the process by verifying the assertion.  If valid, the user will be
	// logged in.  Otherwise, authentication has failed.
	app.get('/auth/google_oauth2/callback', 
		passport.authenticate('google', { successRedirect: '/home',
		                                  successFlash: true,
		                                  failureRedirect: '/',
		                                  failureFlash: true
		                                })
	);

};
