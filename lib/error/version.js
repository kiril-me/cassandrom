
var CassandraError = require('./error.js');

function VersionError () {
  CassandraError.call(this, 'No matching document found.');
  Error.captureStackTrace(this, arguments.callee);
  this.name = 'VersionError';
};

VersionError.prototype.__proto__ = CassandraError.prototype;

module.exports = VersionError;
