/*
 * Express GET handlers and support functions for various site pages
 */
'use strict';

var _ = require('lodash');

/*
 * construct HTML fragment to load preprocessed JavaScript code bundle
 * for specified app script
 */
function makeScriptLoadTag(baseName,pageParams) {
  if (!pageParams)
    pageParams = {};
  var lines = [
    '<script>window.pageParams = ' + JSON.stringify(pageParams) + ';</script>',
    '<script src="/public/js/' + baseName + '.bundle.js"></script>'
  ];
  return lines.join('\n');
} 

function renderSitePage( req, responseHandler, pageTemplate, scriptBaseName, pageParams ) {
  console.log("rendering /home, req.user: ", req.user, "req.flash('info'): ", req.flash('info'), "req.flash('error'): ", req.flash('error'));
  
  if (req.user) {
  
    var templateParams = {
      real_name: req.user.attributes.real_name,
      partials: { pageContent: pageTemplate },
      pageScriptLoad: makeScriptLoadTag(scriptBaseName,pageParams),
      errorMessages: req.flash('error').join(', '),
      infoMessages: req.flash('info').join(', ')
    };

    // augment the basic parameters we'll pass to the template with any page-specific params:
    if (pageParams)
      _.extend(templateParams, pageParams);

    responseHandler.render('site_template', templateParams );
  } else {
    req.flash('error', 'Please log into Google')
    responseHandler.redirect('/');
  }
}

function getHomePage(req, res) {
  return renderSitePage(req,res,'partials/onediv','home');
}

function getOwedEmailPage(req, res) {
  return renderSitePage(req,res,'partials/onediv','owedEmail');
}

function getAllCorrespondentsPage(req, res) {
  return renderSitePage(req,res,'partials/onediv','allCorrespondents');
}

function getCorrespondentRankingsPage(req, res) {
  return renderSitePage(req,res,'partials/correspondentRankings','correspondentRankings');
}


function getCorrespondentPage(req, res) {
  var correspondentName = req.params.correspondentName;

  console.log("getCorrespondentPage: correspondentName = '" + correspondentName + "'");

  var pageParams = {correspondent_name: correspondentName};

  return renderSitePage(req,res,'partials/correspondent','correspondentPage', pageParams);
}

module.exports.getHomePage = getHomePage;
module.exports.getCorrespondentPage = getCorrespondentPage;
module.exports.getAllCorrespondentsPage = getAllCorrespondentsPage;
module.exports.getCorrespondentRankingsPage = getCorrespondentRankingsPage;
module.exports.getOwedEmailPage = getOwedEmailPage;