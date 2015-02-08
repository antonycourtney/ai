/** @jsx React.DOM */

/*
 * top-level JavaScript for IA home page
 */

'use strict';

var _ = require('lodash');
var React = require('react');
var Q = require('q');
var $ = require('jquery');
var moment = require('moment');

// Flux stuff:

var Fluxxor = require('fluxxor');

var constants = require('./constants.js');
var actions = require('./actions.js');
var stores = require('./stores.js');

var queryClient = require('./queryClient.js');

var dataDictionary = require('./data_dictionary.js');
var components = require('./components.js');

var FluxMixin = Fluxxor.FluxMixin(React),
    StoreWatchMixin = Fluxxor.StoreWatchMixin;

var IndexerStatusPanel = React.createClass({

    mixins: [FluxMixin, StoreWatchMixin("StatusStore")],

    getStateFromFlux: function() {
        var store = this.getFlux().store("StatusStore");

        return {
            indexerStatus: store.status
        };
    },

    render: function() {
        var status = this.state.indexerStatus;

        var fmtMessagesIndexed = status.messagesIndexed.toLocaleString();
        var fmtTotalMessages = status.totalMessages.toLocaleString();
    
        var pct = 0;
        if (status.totalMessages) {
            var pct = Math.floor(status.messagesIndexed * 100.0 / status.totalMessages );
        }
        var d = status.lastCompleted;
        var dateCompletedStr = (d == null ? "Never" : d.toLocaleDateString() + " " + d.toLocaleTimeString() );

        // need this to set width of progress bar:
        var styleMap = { width: pct + "%"}
        return (
          <div className="panel panel-default">
            <div className="panel-heading">
              Indexing Status
            </div>
            <div className="panel-body">
              <div className="progress">
                <div className="progress-bar" role="progressbar" aria-valuenow="{pct}" aria-valuemin="0" aria-valuemax="100" style={styleMap}>{pct}%</div>
              </div>
              <div>Analytic Inbox has indexed <strong>{fmtMessagesIndexed}</strong> of your <strong>{fmtTotalMessages}</strong> email messages (<strong>{pct}%</strong>).</div>
              <div>Indexer Status: <strong>{status.statusMessage}</strong></div>
              <div>Last Completed: <strong>{dateCompletedStr}</strong></div>
            </div>
          </div>            
        );
    }
});

module.exports.IndexerStatusPanel = IndexerStatusPanel;
