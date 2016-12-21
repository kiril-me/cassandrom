var Promise = require('bluebird');

// Promise.prototype.exec = function(op, callback) {
//   if (typeof op === 'function') {
//     callback = op;
//   }

//   this.then(
//     function() {
//       callback.apply(null, _results);
//     },
//     function(error) {
//       callback(error);
//     }).
//     catch(function(error) {
//       // TODO
//   });
// };

module.exports = exports = Promise;