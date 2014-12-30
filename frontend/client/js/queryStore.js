/*
 * A Flux store for query results identified by query name
 */

var Fluxxor = require('fluxxor');
var constants = require('./constants.js');

var QueryStore = Fluxxor.createStore({
    initialize: function() {
        this.queryResults = {};

        this.bindActions(
            constants.EVAL_QUERY, this.onEvalQuery,
            constants.EVAL_QUERY_SUCCESS, this.onEvalQuerySuccess,
            constants.EVAL_QUERY_FAIL, this.onEvalQueryFail
        );
    },

    onEvalQuery: function() {
        // not a lot to do yet...could mark as pending
        console.log("onEvalQuery");
    },

    onEvalQuerySuccess: function(payload) {
        console.log("onEvalQuerySuccess: ", payload);
        this.queryResults[payload.key] = { status: true, result: payload.queryResult };
        this.emit("change");
    },

    onEvalQueryFail: function(payload) {
        console.log("onEvalQueryFail: ", payload);
        this.queryResults[payload.key] = { status: false, error: payload.error };
        this.emit("change");
    }
});

module.exports = QueryStore;