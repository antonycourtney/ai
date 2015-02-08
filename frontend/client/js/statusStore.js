/*
 * StatusStore.js
 *
 * A Flux store for user status info
 * 
 */

var Fluxxor = require('fluxxor');
var constants = require('./constants.js');

var StatusStore = Fluxxor.createStore({

  initialize: function() {
    console.log("[StatusStore] initialize")

    this.loading = false;
    this.error = null;
    this.status = { messagesIndexed: 0, totalMessages: 0, statusMessage: "Waiting for update", lastCompleted: null };

    this.bindActions(
      constants.LOAD_STATUS, this.onLoadStatus,
      constants.LOAD_STATUS_SUCCESS, this.onLoadStatusSuccess,
      constants.LOAD_STATUS_FAIL, this.onLoadStatusFail
    );
  },

  onLoadStatus: function() {
    console.log("[StatusStore] onLoadStatus")

    this.loading = true;
    this.emit("change");

  },

  onLoadStatusSuccess: function(payload) {
    console.log("[StatusStore] onLoadStatusSuccess")

    this.loading = false;
    this.status = payload;
    this.error = null;
    this.emit("change");

  },

  onLoadStatusFail: function(payload) {
    console.log("[StatusStore] onLoadStatusFail")

    this.loading = false;
    this.error = payload.error;
    this.emit("change");

  },

});

module.exports = StatusStore;
