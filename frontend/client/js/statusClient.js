/*
 * statusClient.js
 *
 * Fetch status
 *
 */

var Q = require('q');
var $ = require('jquery');


function getStatus() {

    console.log("[getStatus]");

    // Kick off the process of fetching the data
    var statusUrl = "/index/status";
    var promise = Q($.ajax({
        url: statusUrl,
        data: null
    }));

    return promise;

}

module.exports.getStatus = getStatus;
