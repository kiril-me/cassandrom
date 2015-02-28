
var CassandraError = require('./error.js');
var errorMessages = CassandraError.messages;

function ValidatorError (path, msg, type, val) {
  if (!msg) msg = errorMessages.general.default;
  var message = this.formatMessage(msg, path, type, val);
  CassandraError.call(this, message);
  Error.captureStackTrace(this, arguments.callee);
  this.name = 'ValidatorError';
  this.path = path;
  this.type = type;
  this.value = val;
};

ValidatorError.prototype.toString = function () {
  return this.message;
}

ValidatorError.prototype.__proto__ = CassandraError.prototype;

module.exports = ValidatorError;
