/*
 * Flux architecture actions for query evaluation
 *
 */
'use strict';

var constants = require('./constants.js');
var queryClient = require('./queryClient.js');
var statusClient = require('./statusClient.js');

var actions = {

    evalQuery: function(queryName, queryParams) {
        queryParams = queryParams ? queryParams : {};
        var key = queryClient.queryKey(queryName,queryParams);
        var payload = {queryName: queryName, queryParams: queryParams, key: key};
        this.dispatch(constants.EVAL_QUERY, payload);

        queryClient.evalQuery(queryName, queryParams).then(
            function (queryResult) {
                payload.queryResult = queryResult;
                this.dispatch(constants.EVAL_QUERY_SUCCESS, payload);
            }.bind(this),
            function (error) {
                payload.error = error.responseJSON;
                this.dispatch(constants.EVAL_QUERY_FAIL, payload);
            }.bind(this)
        );
    },

    loadStatus: function() {

        // console.log("[actions] loadStatus")

        // Send the action that says we're starting
        this.dispatch(constants.LOAD_STATUS);

        // Kick off the process of fetching the data

        statusClient.getStatus().then(
            function (data) {
                // console.log("[loadStatus] then: ", data);

                // de-serialize Date value
                if (data.lastCompleted != null) {
                    data.lastCompleted = new Date(data.lastCompleted);
                }

                // Fetch the data again in 5 seconds
                window.setTimeout(function () { this.flux.actions.loadStatus() }.bind(this), 5000);

                // Send the load status success action
                this.dispatch(constants.LOAD_STATUS_SUCCESS, data);

            }.bind(this),
            function (error) {

                // Send the load status failure action
                console.log("[loadStatus] error: ", error);

                // Fetch the data again in 30 seconds
                window.setTimeout(function () { this.flux.actions.loadStatus() }.bind(this), 30000);

                this.dispatch(constants.LOAD_STATUS_FAIL, error);

            }.bind(this)
        );
    }
};

module.exports = actions;