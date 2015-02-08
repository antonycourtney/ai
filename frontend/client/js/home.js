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

var isp = require('./indexerStatusPanel.js');

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

    getQueryResult: function(queryName,queryParams) {
        return this.state.queryResults[queryClient.queryKey(queryName,queryParams)];
    },

    render: function() {
        return (
            <div className="row">
                <div className="col-md-2">
                    <isp.IndexerStatusPanel indexerStatus={this.state.indexerStatus} />
                </div>            
                <div className="col-md-10">
                    <components.QueryResultsPanel panelHeading="Your Top Correspondents (Window: Past 1 Year)" 
                        queryResult={this.getQueryResult('topCorrespondents', this.getQueryParams() )} />
                </div>
            </div>
            );
    },

    getQueryParams: function() {
        return { start_date: this.props.startDate};
    },

    componentDidMount: function() {
        var acts = this.getFlux().actions;
        this.getFlux().actions.evalQuery('topCorrespondents', this.getQueryParams());
    }

});

var BaseDashboard = React.createClass({

    mixins: [FluxMixin, StoreWatchMixin("StatusStore")],

    getStateFromFlux: function() {
        var store = this.getFlux().store("StatusStore");

        return {
            indexerStatus: store.status
        };
    },

    render: function() {

        if (this.state.indexerStatus.lastCompleted == null) {
            return (
                <div className="row">
                    <div className="col-md-2">
                        <isp.IndexerStatusPanel indexerStatus={this.state.indexerStatus} />
                    </div>            
                    <div className="col-md-10">
                        <h1> Waiting for indexed data </h1>
                    </div>
                </div>
            );
        } else {
            var startDate = moment().subtract(1,'years').format("YYYY-MM-DD");

            React.render(
                <HomeDashboard flux={this.props.flux} startDate={this.props.startDate} />,
                document.getElementById('main-region')
            );
        }
    },

});

function main() {
    console.log("dataDictionary: ", dataDictionary);

    var flux = new Fluxxor.Flux(stores, actions);

    flux.on("dispatch", function(type, payload) {
        if (console && console.log) {
            console.log("[Dispatch]", type, payload);
        }
    });

    var baseDashboard = React.render(
        <BaseDashboard flux={flux} />,
        document.getElementById('main-region')
    );

    // Kick off getting status for the logged in user
    flux.actions.loadStatus();

}

main();
