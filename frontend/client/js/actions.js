/*
 * Flux architecture actions for query evaluation
 *
 */
var constants = require('./constants.js');
var queryClient = require('./queryClient.js');

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
    }
};

module.exports = actions;