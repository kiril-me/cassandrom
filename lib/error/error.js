
function CassandraError (msg) {
  Error.call(this);
  Error.captureStackTrace(this, arguments.callee);
  this.message = msg;
  this.name = 'CassandraError';
};

/*!
 * Formats error messages
 */

CassandraError.prototype.formatMessage = function (msg, path, type, val) {
  if (!msg) throw new TypeError('message is required');

  return msg.replace(/{PATH}/, path)
            .replace(/{VALUE}/, String(val||''))
            .replace(/{TYPE}/, type || 'declared type');
}

/*!
 * Inherits from Error.
 */

CassandraError.prototype.__proto__ = Error.prototype;

/*!
 * Module exports.
 */

module.exports = exports = CassandraError;

/**
 * The default built-in validator error messages.
 *
 * @see Error.messages #error_messages_MongooseError-messages
 * @api public
 */

CassandraError.messages = require('./messages');

// backward compat
CassandraError.Messages = CassandraError.messages;

/*!
 * Expose subclasses
 */

CassandraError.CastError = require('./cast');
CassandraError.ValidationError = require('./validation')
CassandraError.ValidatorError = require('./validator')
CassandraError.VersionError =require('./version')
CassandraError.OverwriteModelError = require('./overwriteModel')
CassandraError.MissingSchemaError = require('./missingSchema')
CassandraError.DivergentArrayError = require('./divergentArray')
