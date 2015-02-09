/*
 * stores.js
 *
 * All the Flux stores
 *
 */

'use strict';

var QueryStore = require('./queryStore.js');
var StatusStore = require('./statusStore.js');

var stores = {
	QueryStore: new QueryStore(),
	StatusStore: new StatusStore()
};

module.exports = stores;
