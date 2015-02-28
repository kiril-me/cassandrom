
var CassandraError = require('./error.js');

function OverwriteModelError (name) {
  CassandraError.call(this, 'Cannot overwrite `' + name + '` model once compiled.');
  Error.captureStackTrace(this, arguments.callee);
  this.name = 'OverwriteModelError';
};

OverwriteModelError.prototype.__proto__ = CassandraError.prototype;

module.exports = OverwriteModelError;
