var pg = require('pg');
// var conString = process.env.PG_CONN_STRING;
var conString = process.env.AWS_REDSHIFT_FRONTEND_STRING;

pg.connect(conString, function(err, client, done) {
  if(err) {
    return console.error('error fetching client from pool', err);
  }
  client.query('SELECT $1::int AS number', ['93'], function(err, result) {
    //call `done()` to release the client back to the pool
    done();

    if(err) {
      return console.error('error running query', err);
    }
    console.log(result.rows[0].number);
    pg.end();
    //output: 1
  });
});
