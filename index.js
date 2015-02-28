var Schema = require('./schema');
var Model = require('./model');

var cassandraDriver = require('cassandra-driver');
var types = cassandraDriver.types;

function Cassandra() {
  this.connections = [];
  this.models = {};
  this.modelSchemas = {};

  this.uuid = types.uuid;

  this.UUIDType = require('./schema/uuid');
  this.ObjectId = require('./schema/objectid');

  this.options = {

  };
}

Cassandra.prototype.Schema = Schema;


Cassandra.prototype.createConnection = function (options, fn) {
  var conn = new cassandraDriver.Client(options);
  this.connections.push(conn);

  conn.connect(function(error) {
    if(fn) {
      fn(error);
    }
  });
  return conn;
};

Cassandra.prototype.model = function (name, schema, collection, skipInit) {

  if ('string' == typeof schema) {
    collection = schema;
    schema = false;
  }


  // if (utils.isObject(schema) && !(schema instanceof Schema)) {
  //   schema = new Schema(schema);
  // }

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
      throw new mongoose.Error.MissingSchemaError(name);
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
      throw new mongoose.Error.MissingSchemaError(name);
    }
  }

  // Apply relevant "global" options to the schema
  // if (!('pluralization' in schema.options)) {
  //  schema.options.pluralization = this.options.pluralization;
  // }


  if (!collection) {
    collection = schema.get('collection') || format(name, schema.options);
  }

  var connection = this.connections[0]; //options.connection || this.connection;

  // console.log("schema " + JSON.stringify(schema) );

  model = Model.compile(name, schema, collection, connection, this);

  if (!skipInit) {
    model.init();
  }

  if (false === options.cache) {
    return model;
  }

  return this.models[name] = model;
}

var cassandra = module.exports = exports = new Cassandra;
