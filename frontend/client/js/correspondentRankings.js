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
var queryClient = require('./queryClient.js');
var dataDictionary = require('./data_dictionary.js');
var components = require('./components.js');

var FluxMixin = Fluxxor.FluxMixin(React),
    StoreWatchMixin = Fluxxor.StoreWatchMixin;

function renderChart(queryRes) {
  var chartRows = queryRes.result.rows;
  console.log("chartRows: ", chartRows);

  // map from correspondent name to time series
  var seriesMap = {};
  chartRows.map(function (row) {
    var cName = row.correspondentname;
    var cSeries = seriesMap[cName];
    if (!cSeries) {
        cSeries = [];
        seriesMap[cName] = cSeries;
    }
    cSeries.push({cName: cName, date: row.dt, mx: row.mxtrailing7});
  });

  var plotSeries = _.values(seriesMap);

  console.log("plotSeries: ", plotSeries);

  var xScale     = new Plottable.Scale.Time();
  var yScale     = new Plottable.Scale.Linear();
  var colorScale = new Plottable.Scale.Color();

  // Plot Components
  var title  = new Plottable.Component.TitleLabel("Correspondent Rankings", "horizontal" );
  var legend = new Plottable.Component.Legend(colorScale);
  legend.maxEntriesPerRow(1);
  var yLabel = new Plottable.Component.Label("Rank", "left");
  var xAxis  = new Plottable.Axis.Time(xScale, "bottom");
  var yAxis  = new Plottable.Axis.Numeric(yScale, "left");
  var lines  = new Plottable.Component.Gridlines(null, yScale);
  var plots = plotSeries.map(function (cSeries) {
    return new Plottable.Plot.Line(xScale, yScale)
                      .addDataset(cSeries)
                      .project("x", "date", xScale)
                      .project("y", "mx", yScale)
                      .project("stroke", colorScale.scale(cSeries[0].cName))
                      .project("stroke-width", 1);    
  });

  var gridlines = new Plottable.Component.Gridlines(xScale, yScale);
  var center    = new Plottable.Component.Group(plots).merge(lines).merge(legend);
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




var CorrRankingsPage = React.createClass({
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

    getQueryResult: function(queryName,queryParams) {
        return this.state.queryResults[queryClient.queryKey(queryName)];
    },

    render: function() {
        var qres = this.getQueryResult('topRankedMXSeries');
        if (qres) {
            try {
                renderChart(qres);
            } catch (e) {
                console.error("*** Caught exception rendering rankings chart: ", e, e.stack);
            }
        }
        return (
            <div className="row">
                <div className="col-md-12">
                    <components.QueryResultsPanel panelHeading={"Correspondent Historical Rankings"}
                        queryResult={qres} 
                        />
                </div>
            </div>
            );
    },

    componentDidMount: function() {
        var acts = this.getFlux().actions;
        this.getFlux().actions.evalQuery('topRankedMXSeries');
    }

});

function main() {

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
        <CorrRankingsPage flux={flux} />,
        document.getElementById('main-region')
    );
}

main();