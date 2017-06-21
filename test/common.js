Error.stackTraceLimit = 10;

var cassandrom = require('../'),
    assert = require('power-assert'),
    queryCount = 0,
    opened = 0,
    closed = 0;

if (process.env.D === '1') {
  cassandrom.set('debug', true);
}

module.exports = function(options, done) {
  if (typeof options === 'function') {
    done = options;
    options = {};
  }
  options || (options = {});
  var uri;

  if (options.uri) {
    uri = options.uri;
    delete options.uri;
  } else {
    uri = module.exports.uri;
  }

  var keyspace;
  if (options.keyspace) {
    keyspace = options.keyspace;
    delete options.keyspace;
  } else {
    keyspace = module.exports.keyspace;
  }

  var noErrorListener = !!options.noErrorListener;
  delete options.noErrorListener;

  var conn = cassandrom.createConnection({
    contactPoints: [
      uri
    ], keyspace: keyspace,
    createKeyspace: true,
    createTables: true
  }, done);

  if (noErrorListener) {
    return conn;
  }

  conn.on('error', function(err) {
    assert.ok(err);
  });

  return conn;
};

/*!
 * testing uri
 */

module.exports.uri = process.env.CASSANDROM_TEST_URI || 'localhost';
module.exports.keyspace = 'test_keyspace';

/**
 * expose cassandrom
 */

module.exports.cassandrom = cassandrom;

/**
 * expose cassandra version helper
 */

module.exports.cassandraVersion = function(cb) {
  var db = module.exports();
  db.on('error', cb);

  db.on('open', function() {
    var admin = db.db.admin();
    admin.serverStatus(function(err, info) {
      if (err) {
        return cb(err);
      }
      var version = info.version.split('.').map(function(n) {
        return parseInt(n, 10);
      });
      db.close(function() {
        cb(null, version);
      });
    });
  });
};

function dropDBs(done) {
  var db = module.exports({ noErrorListener: true });
  db.once('open', function() {
    // drop the default test database
    db.dropDatabase(function() {
      done();
    });
  });
}

before(function(done) {
  this.timeout(10 * 1000);
  dropDBs(done);
});
after(function(done) {
  //this.timeout(120 * 1000);
  //dropDBs(done);
  done();
});