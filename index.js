
var parallel = require('async/parallel');
var cassandraDriver = require('cassandra-driver');

var Schema = require('./lib/schema');
var Connection = require('./lib/connection');
var utils = require('./lib/utils');
var Promise = require('./lib/promise');
var Model = require('./lib/model');
var error = require('./lib/error');

var log = require('./lib/log');

var types = cassandraDriver.types;
var format = utils.toCollectionName;

var KEYSPACE_DOES_NOT_EXIST_ERROR = 8704;

function Cassandrom() {
  this.connections = [];

  this.models = {};
  this.modelSchemas = {};

  this.uuid = types.uuid;

  this.Types = require('./lib/types');

  this.UUIDType = require('./lib/schema/uuid');
  this.ListId = require('./lib/schema/listid');

  this.options = {

  };
}

Cassandrom.prototype.Schema = Schema;
Cassandrom.prototype.Promise = Promise;

Cassandrom.prototype.connect = Cassandrom.prototype.createConnection = function (options, fn) {
  var keyspace = options.keyspace;
  delete options.keyspace;

  var conn = new Connection(this, options);
  var index = this.connections.length;
  this.connections.push(conn);

  conn._events = {
    error: function() { },
    open: function() { },
  };

  conn.once = function(name, fn) {
    conn._events[name] = fn;
    return this;
  };

  var self = this;

  if(fn && options.createTables) {
    var done = fn;
    fn = function(error, conn) {
      if(error) {
        done(error);
      } else {
        self._createTables(conn, done);
      }
    };
  }
  fn = fn || function() {};

  var connectToKeyspace = function(error) {
    if(error) {
      fn(error, conn);
    } else {
      conn.useKeyspace(keyspace, function(err, result) {
        if(err) {
          if(options.createKeyspace) {
            log.info('Create keyspace ' + keyspace);
            conn.createKeyspace(keyspace, 'SimpleStrategy', 1, function(error) {
              if(error) {
                log.info('Create keyspace Error: ' + error);
                conn._events['error'](error);
                fn(error, conn);
              } else {
                conn.useKeyspace(keyspace, function(err, result) {
                  if(err) {
                    log.info('Could not use ' + keyspace + '  ' + err);
                    conn._events['error'](error);
                  } else {
                    self._setupConnection(conn);
                    conn._events['open']();
                  }
                  fn(error, conn);
                });
              }
            });
          } else {
            conn._events['error'](error);
            fn(error, conn);
          }
        } else {
          self._setupConnection(conn);
          conn._events['open']();
          fn(error, conn);
        }
      });
    }
  };

  conn.connect(function(error) {
    if(error) {
      log.info('Connection Error: ' + error);
      conn._events['error'](error);
    } else {
      log.info('Successfully connected...');
    }
    connectToKeyspace(error);
  });

  conn.onClose(function() {
    self.connections.splice(index, 1);
  });

  this.connection = conn;

  return conn;
};

Cassandrom.prototype.model = function (name, schema, collection, skipInit, cb) {
  if ('string' == typeof schema) {
    collection = schema;
    schema = false;
  }

  if ('boolean' === typeof collection) {
    skipInit = collection;
    collection = null;
  }


  // handle internal options from connection.model()
  var options;
  if (skipInit && utils.isObject(skipInit)) {
    options = skipInit;
    skipInit = true;
  } else {
    options = {};
  }

  // look up schema for the collection. this might be a
  // default schema like system.indexes stored in SchemaDefaults.
  if (!this.modelSchemas[name]) {
    if (!schema && name in SchemaDefaults) {
      schema = SchemaDefaults[name];
    }

    if (schema) {
      // cache it so we only apply plugins once
      this.modelSchemas[name] = schema;
      // this._applyPlugins(schema);
    } else {
      throw new Error.MissingSchemaError(name);
    }
  }

  var model;
  var sub;

  // connection.model() may be passing a different schema for
  // an existing model name. in this case don't read from cache.
  if (this.models[name] && false !== options.cache) {
    if (schema instanceof Schema && schema != this.models[name].schema) {
      throw new error.OverwriteModelError(name);
    }

    if (collection) {
      // subclass current model with alternate collection
      model = this.models[name];
      schema = model.prototype.schema;
      sub = model.__subclass(this.connection, schema, collection);
      // do not cache the sub model
      return sub;
    }

    return this.models[name];
  }

  // ensure a schema exists
  if (!schema) {
    schema = this.modelSchemas[name];
    if (!schema) {
      throw new Error.MissingSchemaError(name);
    }
  }

  if(!schema.primaryKey) {
    throw new Error('Primary Key must be provided');
  }

  if (!collection) {
    collection = schema.get('collection') || name;
  }

  model = Model.compile(name, schema, collection, null);

  if (!skipInit) {
    model.init();
  }

  if(!model.base && this.connection) {
    model.base = model.prototype.base = this.connection;
  }

  if(this.connection._options.createTables) {
    this.connection.createTableIfNotExists(name, model.schema, function(error) {
      if(error) {
        console.error('[cassandrom] Failed create table ' + name + ' ' + error);
      }
      cb && cb(error);
    });
  }
  cb && cb(error);

  if (false === options.cache) {
    return model;
  }

  return this.models[name] = model;
};

Cassandrom.prototype._createTables = function(conn, done) {
  var toExecute = [];
  for(var name in this.models) {
    var schema = this.models[name].schema;
    toExecute.push(function(callback) {
      conn.createTableIfNotExists(name, schema, callback);
    });
  }
  if(toExecute.length > 0) {
    parallel(toExecute, function(error, savedDocs) {
      if(error) {
        console.error('[cassandrom] Could not create table ' + error);
      }
      done(error, conn);
    });
  } else {
    done(null, conn);
  }
};

Cassandrom.prototype._setupConnection = function(connection) {
  for(var name in this.models) {
    var model = this.models[name];
    if(!model.base) {
      model.base = model.prototype.base = connection;
    }
  }
};



var cassandra = module.exports = exports = new Cassandrom;
