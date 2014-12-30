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
    var columnNames = _.pluck(queryRes.result.fields,'name');
    var chartRows = queryRes.result.rows;
    var chartData = [ columnNames ];
    chartData = chartData.concat(chartRows);
    console.log("chartData: ", chartData);

    var dataTable = new google.visualization.DataTable(
        {
            cols: [{id: 'dt', label: 'Date', type: 'date'},
                   {id: 'messagesReceived', label: 'Messages Received', type: 'number'}, 
                   {id: 'messagesSent', label: 'Messages Sent', type: 'number'}]
        });
    console.log("dataTable: ", dataTable);

    var dataRows = chartRows.map(function (r) { return [r.dt, Number.parseInt(r.messagesreceived), Number.parseInt(r.messagessent)]; });

    dataTable.addRows(dataRows);
//    var data = google.visualization.arrayToDataTable(chartData);
    var options = {
      title: 'Messages Exchanged',
      orientation: 'horizontal',
      bar: { groupWidth: '80%' },
      isStacked: true
      /* curveType: 'function' */
    };

    // Let's try a bar chart instead of a line chart:
    // var chart = new google.visualization.LineChart(document.getElementById('chart_div'));
    var chart = new google.visualization.BarChart(document.getElementById('chart_div'));

    console.log("Chart object: ", chart);

    /*
    var dataView = new google.visualization.DataView(dataTable);
    dataView.setColumns([{calc: function(data, row) { return data.getFormattedValue(row, 0); }, type:'string'}, 1]);
    */
    chart.draw(dataTable, options);
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