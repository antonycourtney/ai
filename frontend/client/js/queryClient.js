/*
 * query client for Analytic Inbox
 */
'use strict';

var _ = require('lodash');
var $ = require('jquery');
var Q = require('q');
// only seems to work on node:
// var pgTypes = require('pg-types');

function parseDate(ds) {
    var ret = new Date(ds);
    return ret;
}

var parserRegistry = {};
function registerParser(tid,pf) {
    parserRegistry[tid] = pf;
}

registerParser(1082,parseDate);   // date
registerParser(1114,parseDate);   // timestamp w/o timezone
registerParser(1184,parseDate);   // timestamp

function getTypeParser(tid) {
    var p = parserRegistry[tid];
    if (p === undefined) {
        p = function(s) {
            return s;
        };
    }
    return p;
}

function mapResultTypes(qres) {
    var typeIds = _.pluck(qres.fields,'dataTypeID');
    var fieldNames = _.pluck(qres.fields,'name');
    var parsers = typeIds.map( function (tid) {
        return getTypeParser(tid,'text');
    });

    var mappedRows = qres.rows.map(function (row) {
        var mr = {};
        for (var col = 0; col < fieldNames.length; col++ ) {
            var colName = fieldNames[col];
            var colParser = parsers[col];
            var cellText = row[colName];
            var cellVal = colParser(cellText);
            mr[colName] = cellVal;
        }
        return mr;
    });

    var res = { rows: mappedRows, fields: qres.fields };

    console.log("After mapping result types: ", res);
    return res;
}

function evalQuery(queryName, queryParams) {
    var queryUrl = "/queries/" + queryName;

    console.log("evalQuery: ", queryName, queryParams);

    var promise = Q($.ajax({
        url: queryUrl,
        data: queryParams
    }));

    return promise.then(mapResultTypes).fail(function(err) {
        console.log("Got error: ", err);
    });
}

/* get a key for looking up query in the store */
function queryKey(queryName, queryParams) {
    queryParams = queryParams ? queryParams : {};
    var baseKeyObj = {queryName: queryName, queryParams: queryParams};
    var key = JSON.stringify(baseKeyObj);

    return key;    
}

module.exports.evalQuery = evalQuery;
module.exports.queryKey = queryKey;