
var CassandraError = require('./error.js');


function CastError (type, value, path) {
  CassandraError.call(this, 'Cast to ' + type + ' failed for value "' + value + '" at path "' + path + '"');
  Error.captureStackTrace(this, arguments.callee);
  this.name = 'CastError';
  this.type = type;
  this.value = value;
  this.path = path;
};

CastError.prototype.__proto__ = CassandraError.prototype;

module.exports = CastError;
