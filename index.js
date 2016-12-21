var Schema = require('./schema');
var Model = require('./model');
var Error = require('./error');

var cassandraDriver = require('cassandra-driver');
var types = cassandraDriver.types;

function Cassandrom() {
  this.connections = [];
  this.models = {};
  this.modelSchemas = {};

  this.uuid = types.uuid;

  this.Types = require('./types');

  this.UUIDType = require('./schema/uuid');
  this.ObjectId = require('./schema/objectid');
  this.ListId = require('./schema/listid');

  this.options = {

  };
}

Cassandrom.prototype.Schema = Schema;

Cassandrom.prototype.connect = Cassandrom.prototype.createConnection = function (options, fn) {
  var conn = new cassandraDriver.Client(options);

  this.connections.push(conn);

  conn._events = {
    error: function() { },
    open: function() { },
  };

  conn.once = function(name, fn) {
    conn._events[name] = fn;
    return this;
  };

  conn.connect(function(error) {
    if(error) {
      console.log('[cassandrom] Connection Error: ' + error);
      conn._events['error'](error);
    } else {
      console.log('[cassandrom] Cassandrom successfully connected...');
      conn._events['open']();
    }
    if(fn) {
      fn(error);
    }
  });

  this.connection = conn;

  return conn;
};

Cassandrom.prototype.model = function (name, schema, collection, skipInit) {
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

  if (!collection) {
    collection = schema.get('collection') || format(name, schema.options);
  }

  var connection = this.connections[0]; //options.connection || this.connection;

  // console.log("schema " + JSON.stringify(schema) );

  model = Model.compile(name, schema, collection, this);

  if (!skipInit) {
    model.init();
  }

  if (false === options.cache) {
    return model;
  }

  return this.models[name] = model;
}

Cassandrom.prototype.execute = function() {
  this.connections[0].execute.apply(this.connections[0], arguments);
};

var cassandra = module.exports = exports = new Cassandrom;
