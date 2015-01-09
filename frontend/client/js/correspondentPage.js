/** @jsx React.DOM */

/*
 * top-level JavaScript for IA correspondent page
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

var FluxMixin = Fluxxor.FluxMixin(React),
    StoreWatchMixin = Fluxxor.StoreWatchMixin;


/* temporary hack to render chart using Google Charts */
function renderChart(queryRes) {
  var chartRows = queryRes.result.rows;
  console.log("chartRows: ", chartRows);

  // Let's split this into two series, one each for messages sent and messages received:
  var messagesSent = chartRows.map(function (r) { return {dt: r.dt, messageCount: r.messagessent, label: "Messages Sent" }; });
  var messagesReceived = chartRows.map(function (r) { return {dt: r.dt, messageCount: r.messagesreceived, label: "Messages Received" }; });



  function getXDataValue(d) { return d.dt; }
  function getYDataValue(d) { return d.messageCount; }

  var xScale     = new Plottable.Scale.Time();
  var yScale     = new Plottable.Scale.Linear();
  var colorScale = new Plottable.Scale.Color();

  // Plot Components
  var title  = new Plottable.Component.TitleLabel("Messages Exchanged", "horizontal" );
  var legend = new Plottable.Component.Legend(colorScale);
  legend.maxEntriesPerRow(2);
  var yLabel = new Plottable.Component.Label("Messages Exchanged", "left");
  var xAxis  = new Plottable.Axis.Time(xScale, "bottom");
  var yAxis  = new Plottable.Axis.Numeric(yScale, "left");
  var lines  = new Plottable.Component.Gridlines(null, yScale);
  var plot   = new Plottable.Plot.StackedBar(xScale, yScale)
    .project("x", getXDataValue, xScale)
    .project("y", getYDataValue, yScale)
    .project("fill", function(d){return d.label}, colorScale)
    .addDataset(messagesSent)
    .addDataset(messagesReceived);

  var gridlines = new Plottable.Component.Gridlines(xScale, yScale);
  var center    = new Plottable.Component.Group([plot]).merge(lines).merge(legend);
  var table     = new Plottable.Component.Table([[yLabel, yAxis, center], [null, null, xAxis]]).renderTo(d3.select("svg#chart"));
  var panZoom   = new Plottable.Interaction.PanZoom(xScale, null);
  center.registerInteraction(panZoom);

/*
  // Layout and render
  new Plottable.Component.Table([
    [null,    null, title],
    [null,    null, legend],
    [yLabel, yAxis, lines.merge(plot)],
    [null,    null, xAxis]
  ])
  .renderTo("svg#chart");
*/
}

var CorrPage = React.createClass({
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
    var chartRes=this.getQueryResult('messagesExchangedWithCorrespondentPerMonth', this.getQueryParams());
    console.log("render: chartRes: ", chartRes);
    if (chartRes) {
      try {
        renderChart(chartRes);
      } catch (e) {
        console.log("caught exception rendering chart: ", e, e.stack);
      }
    }
    return (
      <div className="row">
        <div className="col-md-12">
          <components.QueryResultsPanel panelHeading={"Messages From " + this.props.correspondentName + " To You"}
            queryResult={this.getQueryResult('directToUserMessagesFromCorrespondentName', this.getQueryParams())} />
        </div>
      </div>
      );
  },

  componentDidMount: function() {
    var acts = this.getFlux().actions;
    console.log("componentDidMount: actions: ", acts);
    this.getFlux().actions.evalQuery('directToUserMessagesFromCorrespondentName', this.getQueryParams());
    this.getFlux().actions.evalQuery('messagesExchangedWithCorrespondentPerMonth', this.getQueryParams());        
  }

});

function main() {
  console.log("correspondentPage!  pageParams = ", window.pageParams);

  var stores = {
    QueryStore: new QueryStore()
  };

  var flux = new Fluxxor.Flux(stores, actions);

  flux.on("dispatch", function(type, payload) {
    if (console && console.log) {
      console.log("[Dispatch]", type, payload);
    }
  });

  var corrPage = React.renderComponent(
    <CorrPage flux={flux} correspondentName={window.pageParams.correspondent_name} />,
    document.getElementById('main-region')
  );
}

main();