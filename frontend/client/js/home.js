/** @jsx React.DOM */

/*
 * top-level JavaScript for IA home page
 */

'use strict';

var _ = require('lodash');
var React = require('react');
var Q = require('q');
var $ = require('jquery');

// Flux stuff:

var Fluxxor = require('fluxxor');
var constants = require('./constants.js');
var QueryStore = require('./queryStore.js');
var actions = require('./actions.js');
var queryClient = require('./queryClient.js');

var dataDictionary = require('./data_dictionary.js');
var components = require('./components.js');

var IndexerStatusPanel = React.createClass({
    render: function() {
        var status = this.props.indexerStatus;

        var fmtMessagesIndexed = status.messagesIndexed.toLocaleString();
        var fmtTotalMessages = status.totalMessages.toLocaleString();
    
        var pct = 0;
        if (status.totalMessages) {
            var pct = Math.round(status.messagesIndexed / status.totalMessages * 100);
        }
        var d = status.lastCompleted;
        var dateCompletedStr = d.toLocaleDateString() + " " + d.toLocaleTimeString();

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


var FluxMixin = Fluxxor.FluxMixin(React),
    StoreWatchMixin = Fluxxor.StoreWatchMixin;

var HomeDashboard = React.createClass({
    mixins: [FluxMixin, StoreWatchMixin("QueryStore")],    

    getInitialState: function() {
        return {
            indexerStatus: { messagesIndexed: 0, totalMessages: 0, statusMessage: "Waiting for update", 
                             lastCompleted: new Date() }
         };
    },

    getStateFromFlux: function() {
        var store = this.getFlux().store("QueryStore");

        return {
            queryResults: store.queryResults
        };
    },

    getQueryResult: function(queryName) {
        return this.state.queryResults[queryClient.queryKey(queryName)];
    },

    render: function() {
        return (
            <div className="row">
                <div className="col-md-2">
                    <IndexerStatusPanel indexerStatus={this.state.indexerStatus} />
                </div>            
                <div className="col-md-10">
                    <components.QueryResultsPanel panelHeading="Your Top Correspondents" 
                        queryResult={this.getQueryResult('topCorrespondents')} />
                </div>
            </div>
            );
    },

    componentDidMount: function() {
        var acts = this.getFlux().actions;
        console.log("componentDidMount: actions: ", acts);
        this.getFlux().actions.evalQuery('topCorrespondents');
    }

});

/*
 * fetch and update indexerStatus
 */
function fetchIndexerStatus(dashboard) {

    var statusUrl = "/index/status";
    var hp = Q($.ajax({
        url: statusUrl
    }));

    hp.then(function (data) {
        // console.log("Got indexer status:", data);
        data.lastCompleted = new Date(data.lastCompleted);  // de-serialize Date value
        dashboard.setState({ indexerStatus: data });
        if (data.messagesIndexed < data.totalMessages || data.totalMessages === 0) {
            window.setTimeout(function () { fetchIndexerStatus(dashboard) }, 2000);
        } else {
            window.setTimeout(function () { fetchIndexerStatus(dashboard) }, 60000);
        }
    }).catch(function (e) {
        console.error("caught unhandled promise exception: ", e.stack, e);
    });
}

function main() {
    console.log("dataDictionary: ", dataDictionary);
    var stores = {
        QueryStore: new QueryStore()
    };

    var flux = new Fluxxor.Flux(stores, actions);

    flux.on("dispatch", function(type, payload) {
        if (console && console.log) {
            console.log("[Dispatch]", type, payload);
        }
    });

    console.log("Hello, I am the IA home page!");

    var homeDashboard = React.renderComponent(
        <HomeDashboard flux={flux}/>,
        document.getElementById('main-region')
    );

    fetchIndexerStatus(homeDashboard);
}

main();
