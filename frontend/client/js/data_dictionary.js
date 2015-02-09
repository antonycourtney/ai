/** @jsx React.DOM */
/*
 * data dictionary
 * Main job is to map column identifiers used in SQL queries to human-friendly display names
 * in UI.
 */

'use strict';

var _ = require('lodash');
var moment = require('moment');
var React = require('react');

function fmtFromNow(dt) {
    var m = moment(dt);
    return m.fromNow();
}

function fmtCorrespondentName(nm) {
    var enm = encodeURIComponent(nm);
    var url = "/correspondent/" + enm;
    var s = <a href={url}>{nm}</a>;
    return s;
}

// simple identifier to display name mapping:
var simpleCols = {
    'messagesReceived': 'Messages Received',
    'messagesSent': 'Messages Sent',
    'messagesExchanged': 'Messages Exchanged',
    'subject': 'Subject'
};

// columns that map to a descriptor with optional additional information about the column,
// such as description and dataType
var descCols = {
    'correspondentName': {
        displayName: 'Correspondent Name',
        description: "Name used to uniquely identify one of your correspondents, commonly a person's full name",
        formatter: fmtCorrespondentName
    },
    'lastReceived': {
        displayName: 'Last Received',
        formatter: fmtFromNow
    },
    'lastSent': {
        displayName: 'Last Sent',
        formatter: fmtFromNow
    },
    'lastContact': {
        displayName: 'Last Contact',
        formatter: fmtFromNow
    },
    'received': {
        displayName: 'Received'
        // formatter: fmtFromNow
    }
};

// build the data dictionary:
function buildDataDictionary() {
    var dictionary = {
        version: "0.1",

        columns: {}
    };

    dictionary.getDisplayName = function (cid) {
        var entry = this.columns[cid];
        var displayName;
        if (entry === undefined) {
            console.warn("warning: No entry found in data dictionary for column ", cid);
            displayName = cid;
        } else {
            displayName = entry.displayName;
        };
        return displayName;
    };

    dictionary.getFormatter = function(cid) {
        var entry = this.columns[cid];
        var fmt = function (v) { return v.toString(); };
        if (entry!==undefined) {
            if (entry.formatter !== undefined) {
                fmt = entry.formatter;
            }
        }
        return fmt;        
    }

    var columns = {};

    var scs = _.keys(simpleCols);
    for (var i=0; i < scs.length; i++) {
        var cid = scs[i];
        var entry = { displayName: simpleCols[cid] };
        columns[cid.toLowerCase()] = entry;
    }

    var dcs = _.keys(descCols);
    for (var i=0; i < dcs.length; i++) {
        var cid = dcs[i];
        var entry = descCols[cid];
        columns[cid.toLowerCase()] = entry;
    }

    dictionary.columns = columns;
    return dictionary;
}


module.exports = buildDataDictionary();