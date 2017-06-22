var NAME = '[cassandrom]';

var log = {
  info: function(msg) {
    var args = Array.prototype.slice.call(arguments);
    console.log.apply(console, [NAME].concat(args) );
    return log;
  },
  error: function(msg) {
    var args = Array.prototype.slice.call(arguments);
    console.error.apply(console, [NAME].concat(args));
    return log;
  },
  trace: function(msg) {
    var args = Array.prototype.slice.call(arguments);
    console.trace.apply(console, [NAME].concat(args));
    return log;
  },
};

module.exports = log;