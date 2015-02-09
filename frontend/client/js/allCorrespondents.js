/** @jsx React.DOM */

/*
 * top-level JavaScript for page showing all correspondents and their associated email addresses
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
var stores = require('./stores.js');
var queryClient = require('./queryClient.js');
var dataDictionary = require('./data_dictionary.js');
var components = require('./components.js');

var FluxMixin = Fluxxor.FluxMixin(React),
    StoreWatchMixin = Fluxxor.StoreWatchMixin;

var AllCorrPage = React.createClass({
    mixins: [FluxMixin, StoreWatchMixin("QueryStore")],    

    getInitialState: function() {
        return {};
    },

    getStateFromFlux: function() {
        var store = this.getFlux().store("QueryStore");

        return {
            queryResults: store.queryResults
        };
    },

    getQueryParams: function() {
        return { correspondent_name: this.props.correspondentName };
    },

    getQueryResult: function(queryName,queryParams) {
        return this.state.queryResults[queryClient.queryKey(queryName,queryParams)];
    },

    render: function() {
        return (
            <div className="row">
                <div className="col-md-12">
                    <components.QueryResultsPanel panelHeading={"All Correspondents"}
                        queryResult={this.getQueryResult('allCorrespondents')} 
                        collapseRows={true} collapseColCount={2} />
                </div>
            </div>
            );
    },

    componentDidMount: function() {
        var acts = this.getFlux().actions;
        this.getFlux().actions.evalQuery('allCorrespondents');
    }

});

function main() {

    var flux = new Fluxxor.Flux(stores, actions);

    flux.on("dispatch", function(type, payload) {
        if (console && console.log) {
            console.log("[Dispatch]", type, payload);
        }
    });

    var corrPage = React.renderComponent(
        <AllCorrPage flux={flux} />,
        document.getElementById('main-region')
    );
}

main();