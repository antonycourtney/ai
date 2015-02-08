/** @jsx React.DOM */

/*
 * top-level JavaScript for IA home page
 */

'use strict';

var _ = require('lodash');
var React = require('react/addons');
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
    mixins: [FluxMixin, StoreWatchMixin("QueryStore", "StatusStore")],

    getStateFromFlux: function() {
        var queryStore = this.getFlux().store("QueryStore");
        var statusStore = this.getFlux().store("StatusStore");

        return {
            queryResults: queryStore.queryResults,
            indexerStatus: statusStore.status
        };
    },

    getQueryResult: function(queryName,queryParams) {
        return this.state.queryResults[queryClient.queryKey(queryName,queryParams)];
    },

    render: function() {
        var statusPanel =
            <div className="col-md-2">
                <isp.IndexerStatusPanel indexerStatus={this.state.indexerStatus} />
            </div>;

        var mainPanel;

        if (this.state.indexerStatus.lastCompleted == null) {
            mainPanel =
                <div className="col-md-10">
                    <h1> Waiting for indexed data! </h1>
                </div>;
        } else {
            mainPanel = 
                <div className="col-md-10">
                    <components.QueryResultsPanel panelHeading="Your Top Correspondents (Window: Past 1 Year)" 
                        queryResult={this.getQueryResult('topCorrespondents', this.getQueryParams() )} />
                </div>;
        }

        return (
            <div className="row">
                {statusPanel}
                {mainPanel}
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

function main() {
    console.log("dataDictionary: ", dataDictionary);

    var flux = new Fluxxor.Flux(stores, actions);

    // We should replace this when we update fluxxor using the approach outlined in the commit to Fluxxor here:
    // https://github.com/BinaryMuse/fluxxor/commit/fa3ca9fd3cba259a4e3d75bbf3549ee9dd9c381b
    var oldDispatch = flux.dispatcher.dispatch.bind(flux.dispatcher);
    flux.dispatcher.dispatch = function(action) {
      React.addons.batchedUpdates(function() {
        oldDispatch(action);
      });
    };

    flux.on("dispatch", function(type, payload) {
        if (console && console.log) {
            console.log("[Dispatch]", type, payload);
        }
    });

    var startDate = moment().subtract(1,'years').format("YYYY-MM-DD");

    var homeDashboard = React.render(
        <HomeDashboard flux={flux} startDate={startDate} />,
        document.getElementById('main-region')
    );

    // Kick off getting status for the logged in user
    flux.actions.loadStatus();

}

main();
