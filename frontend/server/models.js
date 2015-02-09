/*
 * models.js
 *
 * Models for use with the ORM
 *
 */

'use strict';

// Our Postgres database configuration
var dbConfig = {
  client: 'pg',
  connection: (process.env.DB_PORT ?  {
                                        host     : process.env.DB_PORT_5432_TCP_ADDR,
                                        port     : process.env.DB_PORT_5432_TCP_PORT,
                                        user     : 'glenmistro',
                                        password : '',
                                        database : 'ai_production',
                                      } 
                : process.env.PG_CONN_STRING )
}

console.log("etl: dbConfig is: ", dbConfig);

var knex = require('knex')(dbConfig);

var bookshelf = require('bookshelf')(knex);

// User model
var User = bookshelf.Model.extend({
  tableName: 'users',
  identities: function() {
    return this.hasMany(Identity);
  },
  gmail_syncs: function() {
    return this.hasMany(GmailSync);
  }
});

// Identity model
var Identity = bookshelf.Model.extend({
    tableName: 'identities',
    user: function() {
      return this.belongsTo(User);
    }
});

// GmailSync model
var GmailSync = bookshelf.Model.extend({
  tableName: 'gmail_syncs',
  user: function() {
    return this.belongsTo(User);
  }
});

module.exports.User = User;
module.exports.Identity = Identity;
module.exports.GmailSync = GmailSync;
module.exports.setup = function(app) {
	console.log("models setup");
	app.set('bookshelf', bookshelf);
}
