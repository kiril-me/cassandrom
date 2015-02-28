
var CassandraError = require('./error.js');

function MissingSchemaError (name) {
  var msg = 'Schema hasn\'t been registered for model "' + name + '".\n'
          + 'Use mongoose.model(name, schema)';
  CassandraError.call(this, msg);
  Error.captureStackTrace(this, arguments.callee);
  this.name = 'MissingSchemaError';
};

MissingSchemaError.prototype.__proto__ = CassandraError.prototype;

module.exports = MissingSchemaError;
