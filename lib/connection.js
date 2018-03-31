var cassandraDriver = require('cassandra-driver');
var Client = cassandraDriver.Client;
var RequestHandler = require('cassandra-driver/lib/request-handler');

var parallel = require('async/parallel');

var Schema = require('./schema');
var Model = require('./model');
var Error = require('./error');
var Promise = require('./promise');
var log = require('./log');

function Connection(cassandrom, options) {
  Client.call(this, options);

  this._options = options;
  this.cassandrom = cassandrom;
}

Connection.prototype = Client.prototype;

Connection.prototype.Promise = Promise;

Connection.prototype.dropDatabase = function(cb) {
  this.execute('DROP KEYSPACE ' + this.keyspace, cb);
};

Connection.prototype.useKeyspace = function(keyspace, cb) {
  var self = this;
  this.execute('USE ' + keyspace, function(error, result) {
    if(error) {
      cb(error, result);
    } else {
      self.keyspace = keyspace;
      RequestHandler.setKeyspace(self, function() {
        log.info('Keyspace ' + keyspace + ' changed');
        cb();
      });
    }
  });
};

Connection.prototype.createKeyspace = function(keyspace, strategy, replication, cb) {
  this.execute('CREATE KEYSPACE ' + keyspace
    + ' WITH replication = { \'class\' : \''
    + strategy + '\', \'replication_factor\' : '
    + replication + ' } AND durable_writes = true',
    cb);
};

Connection.prototype.model = function (name, schema, collection, skipInit, cb) {
  if ('function' === typeof collection) {
    cb = collection;
    collection = null;
  } else if('function' === typeof skipInit) {
    cb = skipInit;
    skipInit = null;
  }

  var model = this.cassandrom.model(name, schema, collection, skipInit, cb);
  model.base = model.prototype.base = this;

  return model;
};

Connection.prototype.createTableIfNotExists = function(name, schema, cb) {
  if(schema._tableCreated) {
    cb();
  } else {
    schema._tableCreated = true;
    var self = this;
    this.execute('SELECT table_name FROM system_schema.tables WHERE keyspace_name=? and table_name=?', [this.keyspace, name.toLowerCase()], function(error, result) {
      if(error) {
        schema._tableCreated = false;
        log.info('Could not check table ' + name + ' error: ' + error);
        cb(error);
      } else {
        if(result.rows.length === 0) {
          log.info('Create table ' + name);
          self.createTable(name, schema, function(error) {
            if(error) {
              schema._tableCreated = false;
            }
            cb(error);
          });
        } else {
          log.info('Table exists ' + name);
          cb();
        }
      }
    });
  }
};

Connection.prototype.createTable = function(name, schema, cb) {
  var create = 'CREATE TABLE ';
  create += this.keyspace;
  create += '.';
  create += name;
  create += ' (\n';

  schema.eachPath(function(path, type) {
    create += path.replace('.', '_');
    create += ' ';
    create += type.type;
    create += ',\n';
  });

  create += 'PRIMARY KEY (';
  if(Array.isArray(schema.primaryKey[0])) {
    create += ' ( ';
    for(var i = 0; i < schema.primaryKey[0].length; i++) {
      if(i !== 0) {
        create += ', ';
      }
      create += schema.primaryKey[0][i];
    }
    create += ' ) ';
  } else {
    create += schema.primaryKey[0];
  }

  for(var i = 1; i < schema.primaryKey.length; i++) {
    create += ', ';
    create += schema.primaryKey[i];
  }
  create +=  ')\n)';

  var self = this;
  this.execute(create, function(error, callback) {
    if(error) {
      cb(error);
    } else {
      var toExecute = [];
      for(var i = 0; i < schema._indexes.length; i++) {
        for(var index in schema._indexes[i]) {
          toExecute.push(self._createIndex(name, index));
        }
      }
      if(toExecute.length === 0) {
        cb();
      } else {
        parallel(toExecute, function(error, savedDocs) {
          if(error) {
            console.error('[cassandrom] Could not create index ' + error);
          }
          cb(error);
        });
      }
    }
  });
};

Connection.prototype._createIndex = function(table, index) {
  var self = this;
  var insert = 'CREATE INDEX ON ' + this.keyspace + '.' + table + ' (' + index + ')';
  return function(callback) {
    self.execute(insert, callback);
  };
};

Connection.prototype.close = function(cb) {
  this.shutdown(cb);
  this._onClose && this._onClose();
};

Connection.prototype.onClose = function(cb) {
  this._onClose = cb;
};

module.exports = exports = Connection;