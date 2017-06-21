var Document = require('./document');
var Query = require('./query');
var parallel = require('async/parallel');

var utils = require('./utils');
var error = require('./error');
var log = require('./log');
// var Promise = require('./promise');
var EventEmitter = require('events').EventEmitter;

var VERSION_WHERE = 1,
    VERSION_INC = 2,
    VERSION_ALL = VERSION_WHERE | VERSION_INC;

function Model(doc, fields) {
  Document.call(this, doc, fields);
}

Model.prototype.__proto__ = Document.prototype;

Model.prototype.modelName;

Model.prototype.db;

Model.prototype.collection;

Model.prototype.baseModelName;

for (var i in EventEmitter.prototype) {
  Model[i] = EventEmitter.prototype[i];
}

Model.init = function init () {
  if ((this.schema.options.autoIndex) ||
      (this.schema.options.autoIndex === null && this.db.config.autoIndex)) {
    this.ensureIndexes({ __noPromise: true, _automatic: true });
  }

  this.schema.emit('init', this);
};

Model.ensureIndexes = function ensureIndexes(options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = null;
  }

  if (options && options.__noPromise) {
    _ensureIndexes(this, options, callback);
    return;
  }
/*
  if (callback) {
    callback = this.$wrapCallback(callback);
  }
*/
  var _this = this;
  return new this.base.Promise(function(resolve, reject) {
    _ensureIndexes(_this, options || {}, function(error) {
      if (error) {
        callback && callback(error);
        reject(error);
      }
      callback && callback();
      resolve();
    });
  });
};

function _ensureIndexes(model, options, callback) {
  var indexes = model.schema.indexes();
  if (!indexes.length) {
    setImmediate(function() {
      callback && callback();
    });
    return;
  }
  // TODO index
  console.log('TODO index support');
}

// Model.prototype.getValue = function (path) {
//   console.log(" ##### " + JSON.stringify( this._doc ) + ' path ' + path);
//   return this._doc[path];
//   // return utils.getValue(path, this._doc);
// }

Model.prototype.save = function save (options, cb) {
  if (typeof options === 'function') {
    cb = options;
    options = undefined;
  }

  if (!options) {
    options = {};
  }

  if (cb) {
    cb = this.constructor.$wrapCallback(cb);
  }
  var self = this;
  var promise = new this.base.Promise(function(resolve, reject) {
    self.$__save(options, function(error, result, numAffected) {
      console.log('saved');

      if (error) {
        reject(error);
        cb && cb.apply(_this, error);
        return;
      }

      resolve(result);
      cb && cb.apply(self, [null, result, numAffected]);
    });
  });
  return promise;
};

Model.prototype.$__save = function(options, callback) {
  var _this = this;

  _this.$__handleSave(options, function(error, result) {
    if (error) {
      return _this.schema.s.hooks.execPost('save:error', _this, [_this], { error: error }, function(error) {
        callback(error);
      });
    }

    _this.$__reset();
    // _this.$__storeShard();

    var numAffected = 0;
    if (result) {
      if (Array.isArray(result)) {
        numAffected = result.length;
      } else if (result.result && result.result.n !== undefined) {
        numAffected = result.result.n;
      } else if (result.result && result.result.nModified !== undefined) {
        numAffected = result.result.nModified;
      } else {
        numAffected = result;
      }
    }

    // was this an update that required a version bump?
    if (_this.$__.version && !_this.$__.inserting) {
      var doIncrement = VERSION_INC === (VERSION_INC & _this.$__.version);
      _this.$__.version = undefined;

      if (numAffected <= 0) {
        // the update failed. pass an error back
        var err = new VersionError(_this);
        return callback(err);
      }

      // increment version if was successful
      if (doIncrement) {
        var key = _this.schema.options.versionKey;
        var version = _this.getValue(key) | 0;
        _this.setValue(key, version + 1);
      }
    }

    _this.emit('save', _this, numAffected);
    _this.constructor.emit('save', _this, numAffected);
    callback(null, _this, numAffected);
  });
};

Model.prototype.$__handleSave = function(options, callback) {
  var _this = this;
  if (!options.safe && this.schema.options.safe) {
    options.safe = this.schema.options.safe;
  }
  if (typeof options.safe === 'boolean') {
    options.safe = null;
  }

  if (this.isNew) {
    // send entire doc
    var toObjectOptions = {};

    toObjectOptions.retainKeyOrder = this.schema.options.retainKeyOrder;
    toObjectOptions.depopulate = 1;
    toObjectOptions._skipDepopulateTopLevel = true;
    toObjectOptions.transform = false;

    var obj = this.toObject(toObjectOptions);
/*
    if (!utils.object.hasOwnProperty(obj || {}, '_id')) {
      // documents must have an _id else mongoose won't know
      // what to update later if more changes are made. the user
      // wouldn't know what _id was generated by mongodb either
      // nor would the ObjectId generated my mongodb necessarily
      // match the schema definition.
      setTimeout(function() {
        callback(new Error('document must have an _id before saving'));
      }, 0);
      return;
    }

    this.$__version(true, obj);
*/

    var insert = this.schema.insert(this.modelName, this, options);

    var self = this;
    if(insert.errors) {
      self.isNew = true;
      self.emit('isNew', true);
      _this.constructor.emit('isNew', true);
      callback(insert.errors);
    } else {
      this.base.execute(insert.query, insert.params, {prepare: true}, function(error, status) {
        if(error) {
          self.isNew = true;
          self.emit('isNew', true);
          _this.constructor.emit('isNew', true);

          callback(error);
        } else {
          // self.isNew = false;
          callback(null, self);
        }
      });


      this.$__reset();
      this.isNew = false;
      this.emit('isNew', false);
      this.constructor.emit('isNew', false);
      // Make it possible to retry the insert
      this.$__.inserting = true;

    }

/* TODO
    this.collection.insert(obj, options.safe, function(err, ret) {
      if (err) {
        _this.isNew = true;
        _this.emit('isNew', true);

        callback(err);
        return;
      }

      callback(null, ret);
    });
*/
    this.$__reset();
    this.isNew = false;
    this.emit('isNew', false);
    // Make it possible to retry the insert
    this.$__.inserting = true;
  } else {
    // Make sure we don't treat it as a new object on error,
    // since it already exists
    this.$__.inserting = false;

    var delta = this.$__delta();

    if (delta) {
      if (delta instanceof Error) {
        callback(delta);
        return;
      }

      var where = this.$__where(delta[0]);

      if (where instanceof Error) {
        callback(where);
        return;
      }

      var update = this.schema.update(this.modelName, this, where, delta[1]['$set'], options), self = this;
      if(update.error) {
        callback(update.error);
      } else {
        this.base.execute(update.query, update.params, {prepare: true}, function(error, status) {
          if(error) {
            callback(error);
          } else {
            callback(null, self);
          }
        });
      }

      // this.collection.update(where, delta[1], options.safe, function(err, ret) {
      //   if (err) {
      //     callback(err);
      //     return;
      //   }
      //   callback(null, ret);
      // });
    } else {
      this.$__reset();
      callback();
      return;
    }

    this.emit('isNew', false);
    this.constructor.emit('isNew', false);
  }
};

Model.prototype.$__where = function _where(where) {
  where || (where = {});

  var paths,
      len;

  if (!where.id) {
    where.id = this._doc.id;
  }

  if (this.$__.shardval) {
    paths = Object.keys(this.$__.shardval);
    len = paths.length;

    for (var i = 0; i < len; ++i) {
      where[paths[i]] = this.$__.shardval[paths[i]];
    }
  }

// TODO set id
  if (this._doc.id == null) {
    return new Error('No _id found on document!');
  }

  return where;
};

Model.prototype.$__delta = function() {
  var dirty = this.$__dirty();
  if (!dirty.length && VERSION_ALL !== this.$__.version) return;

  var where = {},
      delta = {},
      len = dirty.length,
      divergent = [],
      d = 0;
/* TODO
  where._id = this._doc._id;
  console.log('delta', where._id);
  if (where._id.toObject) {
    where._id = where._id.toObject({ transform: false, depopulate: true });
  }
*/
  where.id = this._doc.id;
  //console.log('delta', where.id);
  if (where.id.toObject) {
    where.id = where.id.toObject({ transform: false, depopulate: true });
  }

  for (; d < len; ++d) {
    var data = dirty[d];
    var value = data.value;

    var match = checkDivergentArray(this, data.path, value);
    if (match) {
      divergent.push(match);
      continue;
    }

    var pop = this.populated(data.path, true);
    if (!pop && this.$__.selected) {
      // If any array was selected using an $elemMatch projection, we alter the path and where clause
      // NOTE: MongoDB only supports projected $elemMatch on top level array.
      var pathSplit = data.path.split('.');
      var top = pathSplit[0];
      if (this.$__.selected[top] && this.$__.selected[top].$elemMatch) {
        // If the selected array entry was modified
        if (pathSplit.length > 1 && pathSplit[1] == 0 && typeof where[top] === 'undefined') {
          where[top] = this.$__.selected[top];
          pathSplit[1] = '$';
          data.path = pathSplit.join('.');
        }
        // if the selected array was modified in any other way throw an error
        else {
          divergent.push(data.path);
          continue;
        }
      }
    }

    if (divergent.length) continue;

    if (undefined === value) {
      operand(this, where, delta, data, 1, '$unset');
    } else if (value === null) {
      operand(this, where, delta, data, null);
    } else if (value._path && value._atomics) {
      // arrays and other custom types (support plugins etc)
      handleAtomics(this, where, delta, data, value);
    } else if (value._path && Buffer.isBuffer(value)) {
      // MongooseBuffer
      value = value.toObject();
      operand(this, where, delta, data, value);
    } else {
      value = utils.clone(value, {depopulate: 1, _isNested: true});
      operand(this, where, delta, data, value);
    }
  }

  if (divergent.length) {
    return new DivergentArrayError(divergent);
  }

  if (this.$__.version) {
    this.$__version(where, delta);
  }

  return [where, delta];
};

function operand(self, where, delta, data, val, op) {
  // delta
  op || (op = '$set');

  if (!delta[op]) delta[op] = {};
  delta[op][data.path] = val;

  // disabled versioning?
  if (self.schema.options.versionKey === false) return;

  // path excluded from versioning?
  if (shouldSkipVersioning(self, data.path)) return;

  // already marked for versioning?
  if (VERSION_ALL === (VERSION_ALL & self.$__.version)) return;

  switch (op) {
    case '$set':
    case '$unset':
    case '$pop':
    case '$pull':
    case '$pullAll':
    case '$push':
    case '$pushAll':
    case '$addToSet':
      break;
    default:
      // nothing to do
      return;
  }

  // ensure updates sent with positional notation are
  // editing the correct array element.
  // only increment the version if an array position changes.
  // modifying elements of an array is ok if position does not change.

  if (op === '$push' || op === '$pushAll' || op === '$addToSet') {
    self.$__.version = VERSION_INC;
  } else if (/^\$p/.test(op)) {
    // potentially changing array positions
    self.increment();
  } else if (Array.isArray(val)) {
    // $set an array
    // self.increment();
  } else if (/\.\d+\.|\.\d+$/.test(data.path)) {
    // now handling $set, $unset
    // subpath of array
    self.$__.version = VERSION_WHERE;
  }
}

function shouldSkipVersioning(self, path) {
  var skipVersioning = self.schema.options.skipVersioning;
  if (!skipVersioning) return false;

  // Remove any array indexes from the path
  path = path.replace(/\.\d+\./, '.');

  return skipVersioning[path];
}


Model.prototype.$__try = function (fn, scope) {
  var res;
  try {
    fn.call(scope);
    res = true;
  } catch (e) {
    this.$__error(e);
    res = false;
  }
  return res;
};


Model.prototype.$__error = function(error) {
  console.error("[cassandrom] Error: " + error);
};
// Model.prototype.$__shouldModify = function (
//     pathToMark, path, constructing, parts, schema, val, priorVal) {

//   if (this.isNew) return true;

//   if (undefined === val && !this.isSelected(path)) {
//     // when a path is not selected in a query, its initial
//     // value will be undefined.
//     return true;
//   }

//   if (undefined === val && path in this.$__.activePaths.states.default) {
//     // we're just unsetting the default value which was never saved
//     return false;
//   }

//   if (!deepEqual(val, priorVal || this.get(path))) {
//     return true;
//   }

//   if (!constructing &&
//       null != val &&
//       path in this.$__.activePaths.states.default &&
//       deepEqual(val, schema.getDefault(this, constructing))) {
//     // a path with a default was $unset on the server
//     // and the user is setting it to the same value again
//     return true;
//   }
//   return false;
// }
/*
Model.prototype.$__set = function (
    pathToMark, path, constructing, parts, schema, val, priorVal) {

  // var shouldModify = this.$__shouldModify.apply(this, arguments);
  var _this = this;

  // if (shouldModify) {
  //   this.markModified(pathToMark, val);
  // }

  var obj = this._doc
    , i = 0
    , l = parts.length

  for (; i < l; i++) {
    var next = i + 1
      , last = next === l;

    if (last) {
      obj[parts[i]] = val;
    } else {
      if (obj[parts[i]] && 'Object' === obj[parts[i]].constructor.name) {
        obj = obj[parts[i]];
      } else if (obj[parts[i]] && 'EmbeddedDocument' === obj[parts[i]].constructor.name) {
        obj = obj[parts[i]];
      } else if (obj[parts[i]] && Array.isArray(obj[parts[i]])) {
        obj = obj[parts[i]];
      } else {
        obj = obj[parts[i]] = {};
      }
    }
  }
}
*/

Model.create = function create(doc, callback) {
  var args;
  var cb;

  if (Array.isArray(doc)) {
    args = doc;
    cb = callback;
  } else {
    var last = arguments[arguments.length - 1];
    if (typeof last === 'function'  || !last) {
      cb = last;
      args = utils.args(arguments, 0, arguments.length - 1);
    } else {
      args = utils.args(arguments);
    }
  }

  if (cb) {
    cb = this.$wrapCallback(cb);
  }

  var _this = this;

  var promise = new this.base.Promise(function(resolve, reject) {
    if (args.length === 0) {
      setImmediate(function() {
        cb && cb(null);
        resolve(null);
      });
      return;
    }

    var toExecute = [];
    var firstError;
    args.forEach(function(doc) {
      toExecute.push(function(callback) {
        var toSave = doc instanceof _this ? doc : new _this(doc);

        var callbackWrapper = function(error, doc) {
          if (error) {
            if (!firstError) {
              firstError = error;
            }
            return callback(error);
          }
          callback(null, doc);
        };

        // Hack to avoid getting a promise because of
        // $__registerHooksFromSchema
        if (toSave.$__original_save) {
          toSave.$__original_save({ __noPromise: true }, callbackWrapper);
        } else {
          toSave.$__save({ __noPromise: true }, callbackWrapper);
        }

      });
    });

    parallel(toExecute, function(error, savedDocs) {
      if (firstError) {
        if (cb) {
          cb(firstError, savedDocs);
        } else {
          reject(firstError);
        }
        return;
      }

      resolve(savedDocs);

      if (doc instanceof Array) {
        cb && cb.call(_this, null, savedDocs);
      } else {
        cb && cb.apply(_this, [null].concat(savedDocs));
      }
    });
  });


  /**
   * Wrap function to support multiple element.
   */
  if (!(doc instanceof Array)) {
    var oldThen = promise.then;
    promise.then = function(didFulfill, didReject) {
      return oldThen.call(promise, function(val) {
        didFulfill.apply(promise, val);
      }, didReject);
    };
  }
  return promise;
};

Model.update = function update(conditions, doc, options, callback) {
  if (callback) {
    callback = this.$wrapCallback(callback);
  }

  if (conditions instanceof Document) {
    conditions = conditions.toObject();
  } else {
    conditions = utils.clone(conditions, {retainKeyOrder: true});
  }

  options = typeof options === 'function' ? options : utils.clone(options);

  var query = new Query({}, this, {});
  return query.update(conditions, doc, options, callback);
};

Model.$wrapCallback = function(callback) {
  var _this = this;
  return function() {
    try {
      callback.apply(null, arguments);
    } catch (error) {
      _this.emit('error', error);
    }
  };
};

/*

Model.__escape = function(val, timeZone) {
  if (val === undefined || val === null) {
    return 'NULL';
  }

  switch (typeof val) {
    case 'boolean': return (val) ? 'true' : 'false';
    case 'number': return val+'';
  }

  if (val instanceof Date) {
    val = Model.__dateToString(val, timeZone || 'local');
  }

  val = val.replace(/[\0\n\r\b\t\\\'\"\x1a]/g, function(s) {
    switch(s) {
      case "\0": return "\\0";
      case "\n": return "\\n";
      case "\r": return "\\r";
      case "\b": return "\\b";
      case "\t": return "\\t";
      case "\x1a": return "\\Z";
      default: return "\\"+s;
    }
  });
  return "'"+val+"'";
};


Model.__dateToString = function(date, timeZone) {
  var dt = new Date(date);

  if (timeZone != 'local') {
    var tz = convertTimezone(timeZone);

    dt.setTime(dt.getTime() + (dt.getTimezoneOffset() * 60000));
    if (tz !== false) {
      dt.setTime(dt.getTime() + (tz * 60000));
    }
  }

  var year   = dt.getFullYear();
  var month  = zeroPad(dt.getMonth() + 1, 2);
  var day    = zeroPad(dt.getDate(), 2);
  var hour   = zeroPad(dt.getHours(), 2);
  var minute = zeroPad(dt.getMinutes(), 2);
  var second = zeroPad(dt.getSeconds(), 2);
  var millisecond = zeroPad(dt.getMilliseconds(), 3);

  return year + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second + '.' + millisecond;
};

Model.__escapeQuery = function(obj) {
  var values = "";
  for(var p in obj) {
    if(values.length > 0) {
      values += ",";
    }
    values += p + " = " + Model.__escape(obj[p]);
  }
  return values;
};
*/
// Model.findById = function findById (id, fields, options, callback) {
//   return this.findOne({ row: id }, fields, options, callback);
// };

Model.count = function count(conditions, callback) {
  if (typeof conditions === 'function') {
    callback = conditions;
    conditions = {};
  }

  var query = new Query({}, this, {});
  return query.count(conditions, callback);
};

Model.distinct = function distinct(field, conditions, callback) {
  if (typeof conditions === 'function') {
    callback = conditions;
    conditions = {};
  }
  if (callback) {
    callback = this.$wrapCallback(callback);
  }

  var query = new Query({}, this, {});
  return query.distinct(field, conditions, callback);
};

Model.find = function find (conditions, projection, options, callback) {
  if (typeof conditions === 'function') {
    callback = conditions;
    conditions = {};
    projection = null;
    options = null;
  } else if (typeof projection === 'function') {
    callback = projection;
    projection = null;
    options = null;
  } else if (typeof options === 'function') {
    callback = options;
    options = null;
  }

  if (callback) {
    callback = this.$wrapCallback(callback);
  }

  var query = new Query({}, this, {});
  query.select(projection);
  query.setOptions(options);
  return query.find(conditions, callback);
/*
  var self = this;
  var _results;
  var promise = new Promise(function(resolve, reject) {
    var select = self.schema.select(self.modelName, conditions, fields, limit);
    if(!select) {
      resolve();
      return;
    }
    self.base.execute(select.query, select.params, {prepare: true}, function(error, result) {
      if (error) {
        log.info('Query error: ' + error);
        reject(error);
        return;
      }
      self.$__parseData(self, fields, result, limit, function(error, data) {
        _results = [error, data];
        if(error) {
          log.info('Query error: ' + error);
          reject(error);
        } else {
          resolve(data);
        }
      });
    });
  });

  if (callback) {
    promise.then(
      function() {
        callback.apply(null, _results);
      },
      function(error) {
        callback(error);
      }).
      catch(function(error) {
        setImmediate(function() {
          log.info('Error ' + self.model + ', ' + error);
          self.model.emit('error', error);
        });
      });
  }

promise.exec = function(op, callback) {
  if (typeof op === 'function') {
    callback = op;
  }

  this.then(
    function() {
      callback.apply(null, _results);
    },
    function(error) {
      callback(error);
    }).
    catch(function(error) {
      setImmediate(function() {
        self.model.emit('error', error);
      });
  });
};

promise.populate = function(name) {

};

promise.count = function(conditions, callback) {
  return self.count(conditions, callback);
};

promise.limit = function(ll) {
  limit = ll;
};

promise.skip = function(skip) {

};

promise.sort = function(sort) {

};

  return promise;
  */
}


Model.populate = function(docs, paths, callback) {
  var _this = this;
  // if (callback) {
  //   callback = this.$wrapCallback(callback);
  // }

  // normalized paths
  var noPromise = paths && !!paths.__noPromise;
  paths = utils.populate(paths);

  // data that should persist across subPopulate calls
  var cache = {};

  if (noPromise) {
    _populate(this, docs, paths, cache, callback);
  } else {
    return new this.base.Promise(function(resolve, reject) {
      _populate(_this, docs, paths, cache, function(error, docs) {
        if (error) {
          callback && callback(error);
          reject(error);
        } else {
          callback && callback(null, docs);
          resolve(docs);
        }
      });
    });
  }
};

function _populate(model, docs, paths, cache, callback) {
  var pending = paths.length;

  if (pending === 0) {
    return callback(null, docs);
  }

  // each path has its own query options and must be executed separately
  var i = pending;
  var path;
  while (i--) {
    path = paths[i];
    populate(model, docs, path, next);
  }

  function next(err) {
    if (err) {
      return callback(err);
    }
    if (--pending) {
      return;
    }
    callback(null, docs);
  }
}

function populate(model, docs, options, callback) {
  var modelsMap;

  // normalize single / multiple docs passed
  if (!Array.isArray(docs)) {
    docs = [docs];
  }

  if (docs.length === 0 || docs.every(utils.isNullOrUndefined)) {
    return callback();
  }

  modelsMap = getModelsMapForPopulate(model, docs, options);

  var i, len = modelsMap.length,
      mod, match, select, vals = [];

  function flatten(item) {
    // no need to include undefined values in our query
    return undefined !== item;
  }

  var _remaining = len;
  var hasOne = false;
  for (i = 0; i < len; i++) {
    mod = modelsMap[i];
    select = mod.options.select;

    if (mod.options.match) {
      match = utils.object.shallowCopy(mod.options.match);
    } else {
      match = {};
    }

    var ids = utils.array.flatten(mod.ids, flatten);
    ids = utils.array.unique(ids);

    if (ids.length === 0 || ids.every(utils.isNullOrUndefined)) {
      --_remaining;
      continue;
    }

    hasOne = true;
    if (mod.foreignField !== '_id' || !match['_id']) {
      match[mod.foreignField] = { $in: ids };
    }

    var assignmentOpts = {};
    assignmentOpts.sort = mod.options.options && mod.options.options.sort || undefined;
    assignmentOpts.excludeId = excludeIdReg.test(select) || (select && select._id === 0);

    if (assignmentOpts.excludeId) {
      // override the exclusion from the query so we can use the _id
      // for document matching during assignment. we'll delete the
      // _id back off before returning the result.
      if (typeof select === 'string') {
        select = select.replace(excludeIdRegGlobal, ' ');
      } else {
        // preserve original select conditions by copying
        select = utils.object.shallowCopy(select);
        delete select._id;
      }
    }

    if (mod.options.options && mod.options.options.limit) {
      assignmentOpts.originalLimit = mod.options.options.limit;
      mod.options.options.limit = mod.options.options.limit * ids.length;
    }

    var subPopulate = mod.options.populate;
    var query = mod.Model.find(match, select, mod.options.options);
    if (subPopulate) {
      query.populate(subPopulate);
    }
    query.exec(next.bind(this, mod, assignmentOpts));
  }

  if (!hasOne) {
    return callback();
  }

  function next(options, assignmentOpts, err, valsFromDb) {
    if (err) return callback(err);
    vals = vals.concat(valsFromDb);
    _assign(null, vals, options, assignmentOpts);
    if (--_remaining === 0) {
      callback();
    }
  }

  function _assign(err, vals, mod, assignmentOpts) {
    if (err) return callback(err);

    var options = mod.options;
    var _val;
    var lean = options.options && options.options.lean,
        len = vals.length,
        rawOrder = {}, rawDocs = {}, key, val;

    // optimization:
    // record the document positions as returned by
    // the query result.
    for (var i = 0; i < len; i++) {
      val = vals[i];
      if (val) {
        _val = utils.getValue(mod.foreignField, val);
        if (Array.isArray(_val)) {
          var _valLength = _val.length;
          for (var j = 0; j < _valLength; ++j) {
            if (_val[j] instanceof Document) {
              _val[j] = _val[j]._id;
            }
            key = String(_val[j]);
            if (rawDocs[key]) {
              if (Array.isArray(rawDocs[key])) {
                rawDocs[key].push(val);
                rawOrder[key].push(i);
              } else {
                rawDocs[key] = [rawDocs[key], val];
                rawOrder[key] = [rawOrder[key], i];
              }
            } else {
              rawDocs[key] = val;
              rawOrder[key] = i;
            }
          }
        } else {
          if (_val instanceof Document) {
            _val = _val._id;
          }
          key = String(_val);
          if (rawDocs[key]) {
            if (Array.isArray(rawDocs[key])) {
              rawDocs[key].push(val);
              rawOrder[key].push(i);
            } else {
              rawDocs[key] = [rawDocs[key], val];
              rawOrder[key] = [rawOrder[key], i];
            }
          } else {
            rawDocs[key] = val;
            rawOrder[key] = i;
          }
        }
        // flag each as result of population
        if (!lean) {
          val.$__.wasPopulated = true;
        }
      }
    }

    assignVals({
      originalModel: model,
      rawIds: mod.ids,
      localField: mod.localField,
      foreignField: mod.foreignField,
      rawDocs: rawDocs,
      rawOrder: rawOrder,
      docs: mod.docs,
      path: options.path,
      options: assignmentOpts,
      justOne: mod.justOne,
      isVirtual: mod.isVirtual
    });
  }
}

function getModelsMapForPopulate(model, docs, options) {
  var i, doc, len = docs.length,
      available = {},
      map = [],
      modelNameFromQuery = options.model && options.model.modelName || options.model,
      schema, refPath, Model, currentOptions, modelNames, modelName, discriminatorKey, modelForFindSchema;

  var originalOptions = utils.clone(options);
  var isVirtual = false;

  schema = model._getSchema(options.path);

  if (schema && schema.caster) {
    schema = schema.caster;
  }

  if (!schema && model.discriminators) {
    discriminatorKey = model.schema.discriminatorMapping.key;
  }

  refPath = schema && schema.options && schema.options.refPath;

  for (i = 0; i < len; i++) {
    doc = docs[i];

    if (refPath) {
      modelNames = utils.getValue(refPath, doc);
      if (Array.isArray(modelNames)) {
        modelNames = modelNames.filter(function(v) {
          return v != null;
        });
      }
    } else {
      if (!modelNameFromQuery) {
        var modelForCurrentDoc = model;
        var schemaForCurrentDoc;

        if (!schema && discriminatorKey) {
          modelForFindSchema = utils.getValue(discriminatorKey, doc);

          if (modelForFindSchema) {
            modelForCurrentDoc = model.db.model(modelForFindSchema);
            schemaForCurrentDoc = modelForCurrentDoc._getSchema(options.path);

            if (schemaForCurrentDoc && schemaForCurrentDoc.caster) {
              schemaForCurrentDoc = schemaForCurrentDoc.caster;
            }
          }
        } else {
          schemaForCurrentDoc = schema;
        }
        var virtual = modelForCurrentDoc.schema._getVirtual(options.path);

        if (schemaForCurrentDoc && schemaForCurrentDoc.options && schemaForCurrentDoc.options.ref) {
          modelNames = [schemaForCurrentDoc.options.ref];
        } else if (virtual && virtual.options && virtual.options.ref) {
          modelNames = [virtual && virtual.options && virtual.options.ref];
          isVirtual = true;
        } else {
          modelNames = null;
        }
      } else {
        modelNames = [modelNameFromQuery];  // query options
      }
    }

    if (!modelNames) {
      continue;
    }

    if (!Array.isArray(modelNames)) {
      modelNames = [modelNames];
    }

    virtual = model.schema._getVirtual(options.path);
    var localField = virtual && virtual.options ?
      (virtual.$nestedSchemaPath ? virtual.$nestedSchemaPath + '.' : '') + virtual.options.localField :
      options.path;
    var foreignField = virtual && virtual.options ?
      virtual.options.foreignField :
      '_id';
    var justOne = virtual && virtual.options && virtual.options.justOne;
    if (virtual && virtual.options && virtual.options.ref) {
      isVirtual = true;
    }

    if (virtual && (!localField || !foreignField)) {
      throw new Error('If you are populating a virtual, you must set the ' +
        'localField and foreignField options');
    }

    options.isVirtual = isVirtual;
    var ret = convertTo_id(utils.getValue(localField, doc));
    var id = String(utils.getValue(foreignField, doc));
    options._docs[id] = Array.isArray(ret) ? ret.slice() : ret;
    if (doc.$__) {
      doc.populated(options.path, options._docs[id], options);
    }

    var k = modelNames.length;
    while (k--) {
      modelName = modelNames[k];

      Model = originalOptions.model && originalOptions.model.modelName ?
        originalOptions.model :
        model.base.model(modelName);

      if (!available[modelName]) {
        currentOptions = {
          model: Model
        };

        if (isVirtual && virtual.options && virtual.options.options) {
          currentOptions.options = utils.clone(virtual.options.options, {
            retainKeyOrder: true
          });
        }
        utils.merge(currentOptions, options);
        if (schema && !discriminatorKey) {
          currentOptions.model = Model;
        }
        options.model = Model;

        available[modelName] = {
          Model: Model,
          options: currentOptions,
          docs: [doc],
          ids: [ret],
          // Assume only 1 localField + foreignField
          localField: localField,
          foreignField: foreignField,
          justOne: justOne,
          isVirtual: isVirtual
        };
        map.push(available[modelName]);
      } else {
        available[modelName].docs.push(doc);
        available[modelName].ids.push(ret);
      }
    }
  }

  return map;
}

function convertTo_id(val) {
  if (val instanceof Model) return val._id;

  if (Array.isArray(val)) {
    for (var i = 0; i < val.length; ++i) {
      if (val[i] instanceof Model) {
        val[i] = val[i]._id;
      }
    }
    if (val.isMongooseArray) {
      return val._schema.cast(val, val._parent);
    }

    return [].concat(val);
  }

  return val;
}


Model.findOne = function findOne (conditions, projection, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = null;
  } else if (typeof projection === 'function') {
    callback = projection;
    projection = null;
    options = null;
  } else if (typeof conditions === 'function') {
    callback = conditions;
    conditions = {};
    projection = null;
    options = null;
  }

  var query = new Query({}, this, {});
  query.select(projection);
  query.setOptions(options);
  return query.findOne(conditions, callback);
};

Model.findById = function findById(id, projection, options, callback) {
  if (typeof id === 'undefined') {
    id = null;
  }

  return this.findOne({_id: id}, projection, options, callback);
};
/*
Model.$__result = function(callback, limit) {
  var self = this;
  return function(error, result) {

    if(error) {
      log.info('Query error: ' + error);
      callback(error);
    } else {
      self.$__parseData(self, undefined, result, limit, callback);
    }
  };
};
*/
/*
function completeOne(model, doc, res, fields, self, pop, callback) {
  var opts = pop ?
  {populated: pop}
      : undefined;

console.log('completeOne model ' + model._);
  var casted = helpers.createModel(model, doc, fields);
  console.log('completeOne ' + casted._);
  casted.init(doc, opts, function(err) {
    if (err) {
      return callback(err);
    }
    console.log('completeOne  init ' + casted._);
    if (res) {
      return callback(null, casted, res);
    }
    callback(null, casted);
  });
}
*/
/*

Model.$__parseData = function(model, fields, data, limit, callback) {
  var list = [];
  if(data.rows && data.rows.length > 0) {
    var i = 0, obj, p, size = (limit  && limit > 0) ? limit : data.rows.length;

    // var len = count;
    // function init(err) {
    //   if (err) return callback(err);
    //   --count || callback(null, arr);
    // }
    var count = size;
    for(; i < size; i++) {
      // undefined, fields, true
      list.push( obj );
      obj = new model( undefined, fields, true); //data.rows[i] );
      obj.init(data.rows[i], undefined, function(err) {
        --count;
        if (err) {
          return callback(err);
        }
        if(count === 0) {
          callback(null, limit === 1 ? obj : list);
        }
      });

  // new model(undefined, fields, true);


    }
  } else {
    callback(null, limit === 1 ? null : []);
  }
  // if(list.length > 0) {
  //   if(one) {
  //     return list[0];
  //   } else {
  //     return list;
  //   }
  // }
  // return null;
};
*/
Model.prototype.$__setSchema = function (schema) {
  compileObject(schema.tree, this);
  this.schema = schema;
}

Model.prototype.$__setModelName = function (modelName) {
  this.modelName = modelName;
}

Model._getSchema = function _getSchema(path) {
  return this.schema._getSchema(path);
};

Model.compile = function compile (name, schema, collectionName, base) {
  // generate new class
  function model (doc, fields, skipId) {
    if (!(this instanceof model)) {
      return new model(doc, fields, skipId);
    }
    Model.call(this, doc, fields, skipId);
  };

  model.hooks = schema.s.hooks.clone();
  model.base = model.prototype.base = base;
  model.modelName = name;

  if (!(model.prototype instanceof Model)) {
    model.__proto__ = Model;
    model.prototype.__proto__ = Model.prototype;
  }

  model.model = Model.prototype.model;
  // model.db = model.prototype.db = connection;

  model.prototype.$__setSchema(schema);

  model.prototype.$__setModelName(name);
  // model.modelName = model.prototype.modelName;

  // apply methods and statics
  applyMethods(model, schema);
  applyStatics(model, schema);

  model.schema = model.prototype.schema;
  model.collection = model.prototype.collection;

  model.options = model.prototype.options;

  return model;
};

function compileObject (tree, proto, prefix) {
  var keys = Object.keys(tree)
    , i = keys.length
    , limb
    , key;

  while (i--) {
    key = keys[i];
    limb = tree[key];

    define(key
        , (('Object' === limb.constructor.name
               && Object.keys(limb).length)
               && (!limb.type || limb.type.type)
               ? limb
               : null)
        , proto
        , prefix
        , keys);
  }
};

function define (prop, subprops, prototype, prefix, keys) {
  var prefix = prefix || ''
    , path = (prefix ? prefix + '.' : '') + prop;

  if (subprops) {

    Object.defineProperty(prototype, prop, {
        enumerable: true
      , configurable: true
      , get: function () {
          if (!this.$__.getters)
            this.$__.getters = {};

          if (!this.$__.getters[path]) {
            var nested = Object.create(Object.getPrototypeOf(this), getOwnPropertyDescriptors(this));

            // save scope for nested getters/setters
            if (!prefix) nested.$__.scope = this;

            // shadow inherited getters from sub-objects so
            // thing.nested.nested.nested... doesn't occur (gh-366)
            var i = 0
              , len = keys.length;

            for (; i < len; ++i) {
              // over-write the parents getter without triggering it
              Object.defineProperty(nested, keys[i], {
                  enumerable: false   // It doesn't show up.
                , writable: true      // We can set it later.
                , configurable: true  // We can Object.defineProperty again.
                , value: undefined    // It shadows its parent.
              });
            }

            nested.toObject = function () {
              return this.get(path);
            };

            compileObject(subprops, nested, path);
            this.$__.getters[path] = nested;
          }

          return this.$__.getters[path];
        }
      , set: function (v) {
          if (v instanceof Document) {
            v = v.toObject();
          }
          return (this.$__.scope || this).set(path, v);
        }
    });

  } else {
    Object.defineProperty(prototype, prop, {
        enumerable: true
      , configurable: true
      , get: function ( ) {
          return this.get.call(this, path);
        }
      , set: function (v) {
        return this.set.call(this, path, v);
      }
    });
  }
};

Model.__subclass = function subclass(conn, schema, collection) {
  // subclass model using this connection and collection name
  var _this = this;

  var Model = function Model(doc, fields, skipId) {
    if (!(this instanceof Model)) {
      return new Model(doc, fields, skipId);
    }
    _this.call(this, doc, fields, skipId);
  };

  Model.__proto__ = _this;
  Model.prototype.__proto__ = _this.prototype;
  Model.base = Model.prototype.base = conn;

  var s = schema && typeof schema !== 'string'
      ? schema
      : _this.prototype.schema;

  var options = s.options || {};

  if (!collection) {
    collection = _this.prototype.schema.get('collection')
        || utils.toCollectionName(_this.modelName, options);
  }

  var collectionOptions = {
    bufferCommands: s ? options.bufferCommands : true,
    capped: s && options.capped
  };

  Model.collection = Model.prototype.collection;
  Model.init();
  return Model;
};


// Model.prototype.toObject = function (options) {
//   if (options && options.depopulate /* && this.$__.wasPopulated */ ) {
//     // populated paths that we set to a document
//     return utils.clone(this._id, options);
//   }

//   // When internally saving this document we always pass options,
//   // bypassing the custom schema options.
//   var optionsParameter = options;
//   if (!(options && 'Object' == options.constructor.name) ||
//       (options && options._useSchemaOptions)) {
//     options = this.schema.options.toObject
//       ? utils.clone(this.schema.options.toObject)
//       : {};
//   }

//   ;('minimize' in options) || (options.minimize = this.schema.options.minimize);
//   if (!optionsParameter) {
//     options._useSchemaOptions = true;
//   }

//   var ret = utils.clone(this._doc, options);

//   if (options.virtuals || options.getters && false !== options.virtuals) {
//     applyGetters(this, ret, 'virtuals', options);
//   }

//   if (options.getters) {
//     applyGetters(this, ret, 'paths', options);
//     // applyGetters for paths will add nested empty objects;
//     // if minimize is set, we need to remove them.
//     if (options.minimize) {
//       ret = minimize(ret) || {};
//     }
//   }

//   // In the case where a subdocument has its own transform function, we need to
//   // check and see if the parent has a transform (options.transform) and if the
//   // child schema has a transform (this.schema.options.toObject) In this case,
//   // we need to adjust options.transform to be the child schema's transform and
//   // not the parent schema's
//   if (true === options.transform ||
//       (this.schema.options.toObject && options.transform)) {
//     var opts = options.json
//       ? this.schema.options.toJSON
//       : this.schema.options.toObject;
//     if (opts) {
//       options.transform = opts.transform;
//     }
//   }

//   if ('function' == typeof options.transform) {
//     var xformed = options.transform(this, ret, options);
//     if ('undefined' != typeof xformed) ret = xformed;
//   }

//   return ret;
// };

Model.prototype.toJSON = function (options) {
  // check for object type since an array of documents
  // being stringified passes array indexes instead
  // of options objects. JSON.stringify([doc, doc])
  // The second check here is to make sure that populated documents (or
  // subdocuments) use their own options for `.toJSON()` instead of their
  // parent's
  if (!(options && 'Object' == options.constructor.name)
      || ((!options || options.json) && this.schema.options.toJSON)) {
    options = this.schema.options.toJSON
      ? utils.clone(this.schema.options.toJSON)
      : {};
  }
  options.json = true;

  return this.toObject(options);
};


function getOwnPropertyDescriptors(object) {
  var result = {};

  Object.getOwnPropertyNames(object).forEach(function(key) {
    result[key] = Object.getOwnPropertyDescriptor(object, key);
    result[key].enumerable = true;
  });

  return result;
}

function minimize (obj) {
  var keys = Object.keys(obj)
    , i = keys.length
    , hasKeys
    , key
    , val

  while (i--) {
    key = keys[i];
    val = obj[key];

    if (utils.isObject(val)) {
      obj[key] = minimize(val);
    }

    if (undefined === obj[key]) {
      delete obj[key];
      continue;
    }

    hasKeys = true;
  }

  return hasKeys
    ? obj
    : undefined;
}


function applyGetters (self, json, type, options) {
  var schema = self.schema
    , paths = Object.keys(schema[type])
    , i = paths.length
    , path

  while (i--) {
    path = paths[i];

    var parts = path.split('.')
      , plen = parts.length
      , last = plen - 1
      , branch = json
      , part

    for (var ii = 0; ii < plen; ++ii) {
      part = parts[ii];
      if (ii === last) {
        branch[part] = utils.clone(self.get(path), options);
      } else {
        branch = branch[part] || (branch[part] = {});
      }
    }
  }

  return json;
}


/*!
 * Register methods for this model
 *
 * @param {Model} model
 * @param {Schema} schema
 */
var applyMethods = function(model, schema) {
  function apply(method, schema) {
    Object.defineProperty(model.prototype, method, {
      get: function() {
        var h = {};
        for (var k in schema.methods[method]) {
          h[k] = schema.methods[method][k].bind(this);
        }
        return h;
      },
      configurable: true
    });
  }
  for (var method in schema.methods) {
    if (typeof schema.methods[method] === 'function') {
      model.prototype[method] = schema.methods[method];
    } else {
      apply(method, schema);
    }
  }
};

/*!
 * Register statics for this model
 * @param {Model} model
 * @param {Schema} schema
 */
var applyStatics = function(model, schema) {
  for (var i in schema.statics) {
    model[i] = schema.statics[i];
  }
};

function checkDivergentArray(doc, path, array) {
  // see if we populated this path
  var pop = doc.populated(path, true);

  if (!pop && doc.$__.selected) {
    // If any array was selected using an $elemMatch projection, we deny the update.
    // NOTE: MongoDB only supports projected $elemMatch on top level array.
    var top = path.split('.')[0];
    if (doc.$__.selected[top + '.$']) {
      return top;
    }
  }

  if (!(pop && array && Array.isArray(array))) {
    return;
  }

  // If the array was populated using options that prevented all
  // documents from being returned (match, skip, limit) or they
  // deselected the _id field, $pop and $set of the array are
  // not safe operations. If _id was deselected, we do not know
  // how to remove elements. $pop will pop off the _id from the end
  // of the array in the db which is not guaranteed to be the
  // same as the last element we have here. $set of the entire array
  // would be similarily destructive as we never received all
  // elements of the array and potentially would overwrite data.
  var check = pop.options.match ||
      pop.options.options && hasOwnProperty(pop.options.options, 'limit') || // 0 is not permitted
      pop.options.options && pop.options.options.skip || // 0 is permitted
      pop.options.select && // deselected _id?
      (pop.options.select._id === 0 ||
      /\s?-_id\s?/.test(pop.options.select));

  if (check) {
    var atomics = array._atomics;
    if (Object.keys(atomics).length === 0 || atomics.$set || atomics.$pop) {
      return path;
    }
  }
}

module.exports = exports = Model;