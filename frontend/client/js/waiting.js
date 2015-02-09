/** @jsx React.DOM */

/*
 * Waiting page for new users before going to the IA home page
 */

'use strict';

var React = require('react/addons');
var $ = require('jquery');

// Flux stuff:

var Fluxxor = require('fluxxor');

var constants = require('./constants.js');
var actions = require('./actions.js');
var stores = require('./stores.js');

var FluxMixin = Fluxxor.FluxMixin(React),
    StoreWatchMixin = Fluxxor.StoreWatchMixin;

var isp = require('./indexerStatusPanel.js');

var WaitingDashboard = React.createClass({
    mixins: [FluxMixin, StoreWatchMixin("StatusStore")],

    getStateFromFlux: function() {
        var statusStore = this.getFlux().store("StatusStore");

        return {
            indexerStatus: statusStore.status
        };
    },

    render: function() {
        var statusPanel =
            <div className="col-md-2">
                <isp.IndexerStatusPanel indexerStatus={this.state.indexerStatus} />
            </div>;

        var mainPanel = null;

        // Make sure we've created the base tables and fetched some data before continuing
        if ((this.state.indexerStatus.created_base_tables == null) ||
            (this.state.indexerStatus.lastCompleted == null))
        {
            mainPanel =
                <div className="col-md-10">
                    <h1> We are collecting your email. </h1>
                    <h3> Please give us a little time to analyse it. </h3>
                </div>;
        } else {
            // Redirect to the home page
            window.location = "/home";
        }

        return (
            <div className="row">
                {statusPanel}
                {mainPanel}
            </div>
            );
    }

});

function main() {

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

    var waitingDashboard = React.render(
        <WaitingDashboard flux={flux} />,
        document.getElementById('main-region')
    );

    // Kick off getting status for the logged in user
    flux.actions.loadStatus();

}

main();
