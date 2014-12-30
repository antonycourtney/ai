/** @jsx React.DOM */
/*
 * React components used in multiple views
 */
'use strict';

var _ = require('lodash');
var React = require('react');
var Q = require('q');
var $ = require('jquery');

// Flux stuff:
/*
var Fluxxor = require('fluxxor');
var constants = require('./constants.js');
var QueryStore = require('./queryStore.js');
var actions = require('./actions.js');
*/

var dataDictionary = require('./data_dictionary.js');

/**
 * group all rows that share the same values for the specified first N columns
 *
 * @param {array} cids column ids for each column
 * @param {array} qrows array of rows to be grouped
 * @param {number} colCount number of initial columns to compare for grouping 
 *
 * @return {array} array of arrays of row groups
 */
function groupRows(cids,qrows,colCount) {
    var allRowGroups = [];
    var rowGroup = [];
    for (var i = 0; i < qrows.length; i++ ) {
        var qrow = qrows[i];
        if (rowGroup.length === 0) {
            // empty group, start a new one:
            rowGroup = [ qrow ];
        } else {
            // Do we match some initial prefix of the previous row?
            var prevRow = rowGroup[rowGroup.length - 1];
            var allMatch = true;
            for ( var j = 0; j < colCount; j++ ) {
                var cid = cids[j];
                if (prevRow[cid]!==qrow[cid]) {
                    allMatch = false;
                    break;
                }
            }
            if (allMatch) { 
                // all rows match, add this row to rowgroup:
                rowGroup.push(qrow);
            } else {
                // row does not match, push old and create new
                allRowGroups.push(rowGroup);
                rowGroup = [qrow];
            }
        }
    }
    if (rowGroup.length > 0)
        allRowGroups.push(rowGroup);
    return allRowGroups;    
}


var QueryResultsPanel = React.createClass({
    renderResultsTable: function(queryResult) {

        var res;
        if (!queryResult) {
            res = <span>Loading...</span>;
        } else if (!queryResult.status) {
            res = <div><strong>Error:</strong> <span>{queryResult.error}</span></div>;
        } else {
            var cids = _.pluck(queryResult.result.fields,'name');

            // make an html row
            var mkRow = function(rowData,collapseColCount,isFirstRow,firstRowSpan) {
                var rowcids = cids;
                if (!isFirstRow && collapseColCount) {
                    rowcids = rowcids.slice(collapseColCount);    
                }
                var htmlRowData = rowcids.map(function (cid,colIdx) {
                    var rowSpan = (isFirstRow && colIdx < collapseColCount) ? firstRowSpan : 1;
                    var fmt = dataDictionary.getFormatter(cid);
                    return (<td rowSpan={rowSpan}>{fmt(rowData[cid])}</td>); 
                });
                return (<tr>{htmlRowData}</tr>);
            }
            var self = this;

            var displayNames = cids.map( function (cid) { return dataDictionary.getDisplayName(cid); } );
            var tableHeaders=displayNames.map(function (displayName) {
                        return (<th>{displayName}</th>);
                    });
            if (this.props.collapseRows) {
                var rowGroups = groupRows(cids,queryResult.result.rows,this.props.collapseColCount);
                var htmlRowGroups = rowGroups.map(function (g) {
                    var firstRow = mkRow(g[0],self.props.collapseColCount,true,g.length);
                    var restRows = g.slice(1).map(function (rowData) {
                        return mkRow(rowData,self.props.collapseColCount,false);
                    });
                    return [firstRow].concat(restRows);
                });
                var  bodyRows= _.flatten(htmlRowGroups, true);
            } else {
                var bodyRows=queryResult.result.rows.map(function (rowData) { return mkRow(rowData); });
            };
            res = 
                (<table className="table table-condensed">
                    <thead>
                        <tr>{tableHeaders}</tr>
                    </thead>
                    <tbody>
                        {bodyRows}
                    </tbody>
                </table>);
        }
        return res;
    },

    render: function() {
        var table = this.renderResultsTable(this.props.queryResult);
        return (
          <div className="panel panel-default query-panel">
            <div className="panel-heading">
              {this.props.panelHeading}
            </div>
            <div className="panel-body">
            {table}  
            </div>
          </div>);  
}
});

module.exports.QueryResultsPanel = QueryResultsPanel;