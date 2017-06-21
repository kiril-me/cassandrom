
var SchemaType = require('../schematype')
  , CastError = SchemaType.CastError
  , errorMessages = require('../error').messages
  , utils = require('../utils')
  , uuidGenerator = require('node-uuid')
  , Document
  , VALIDATE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

var cassandraDriver = require('cassandra-driver');
var Uuid = cassandraDriver.types.Uuid;


function SchemaUUID (key, options) {
  SchemaType.call(this, key, options, 'UUID');
  this.type = 'uuid';
};

SchemaUUID.prototype.__proto__ = SchemaType.prototype;


/**
 * Check required
 *
 * @param {String|null|undefined} value
 * @api private
 */

SchemaUUID.prototype.checkRequired = function checkRequired (value, doc) {
  if (SchemaType._isRef(this, value, doc, true)) {
    return null != value;
  } else if(value instanceof Uuid) {
    return true;
  } else {
    return (value instanceof String || typeof value == 'string') && value.length && VALIDATE.test(value);
  }
};

/**
 * Casts to String
 *
 * @api private
 */

SchemaUUID.prototype.cast = function (value, doc, init) {
  if (SchemaType._isRef(this, value, doc, init)) {
    // wait! we may need to cast this to a document

    if (null == value) {
      return value;
    }

    // lazy load
    Document || (Document = require('./../document'));

    if (value instanceof Document) {
      value.$__.wasPopulated = true;
      return value;
    }

    // setting a populated path
    if (value instanceof Uuid) {
      return value;
    } else if ('string' == typeof value) {
      return Uuid.fromString(value);
    } else if (Buffer.isBuffer(value) || !utils.isObject(value)) {
      throw new CastError('UUID', value, this.path);
    }

    // Handle the case where user directly sets a populated
    // path to a plain object; cast to the Model used in
    // the population query.
    var path = doc.$__fullPath(this.path);
    var owner = doc.ownerDocument ? doc.ownerDocument() : doc;
    var pop = owner.populated(path, true);
    var ret = new pop.options.model(value);
    ret.$__.wasPopulated = true;
    return ret;
  }

  if (value === null) {
    return value;
  }

  if ('undefined' !== typeof value) {
    if(value instanceof Uuid) {
      return value;
    } else if ('string' == typeof value) {
      return value;
    }
  }
  throw new CastError('UUID', value, this.path);
};

/*!
 * ignore
 */

function handleSingle (val) {
  return this.castForQuery(val);
}

function handleArray (val) {
  var self = this;
  return val.map(function (m) {
    return self.castForQuery(m);
  });
}

SchemaUUID.prototype.$conditionalHandlers = {
    '$ne' : handleSingle
  , '$in' : handleArray
  , '$nin': handleArray
  , '$gt' : handleSingle
  , '$lt' : handleSingle
  , '$gte': handleSingle
  , '$lte': handleSingle
  , '$all': handleArray
  , '$regex': handleSingle
  , '$options': handleSingle
};

/**
 * Casts contents for queries.
 *
 * @param {String} $conditional
 * @param {any} [val]
 * @api private
 */

SchemaUUID.prototype.castForQuery = function ($conditional, val) {
  var handler;
  if (arguments.length === 2) {
    handler = this.$conditionalHandlers[$conditional];
    if (!handler)
      throw new Error("Can't use " + $conditional + " with String.");
    return handler.call(this, val);
  } else {
    val = $conditional;
    if (val instanceof RegExp) return val;
    return this.cast(val);
  }
};

/*!
 * Module exports.
 */

module.exports = SchemaUUID;
