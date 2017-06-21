var NAME = '[cassandrom]';

var log = {
  info: function(msg) {
    console.log(NAME, msg);
    return log;
  },
  error: function(msg) {
    console.error(NAME, msg);
    return log;
  },
  trace: function(msg) {
    console.trace(NAME, msg);
    return log;
  },
};

module.exports = log;