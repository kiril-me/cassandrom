var Promise = require('bluebird');
var utils = require('./utils');
//var cast = require('./cast');

var OPERATIONS = { '$in': true };

function Query(conditions, model, options) {
  this.model = model
  this.schema = model.schema;
  this._fields = null;
  this.options = {
   // populate: {}
  };
  this._conditions = conditions;

  this._limit = null;
  this._skip = null;
  this._sort = null;
  // if(conditions) {
  //   this.find(conditions);
  // }

  if (this.schema) {
    var kareemOptions = { useErrorHandlers: true };
    this._count = this.model.hooks.createWrapper('count',
        Query.prototype._count, this, kareemOptions);
    this._execUpdate = this.model.hooks.createWrapper('update',
        Query.prototype._execUpdate, this, kareemOptions);
    this._find = this.model.hooks.createWrapper('find',
        Query.prototype._find, this, kareemOptions);
    this._findOne = this.model.hooks.createWrapper('findOne',
        Query.prototype._findOne, this, kareemOptions);
    this._findOneAndRemove = this.model.hooks.createWrapper('findOneAndRemove',
        Query.prototype._findOneAndRemove, this, kareemOptions);
    this._findOneAndUpdate = this.model.hooks.createWrapper('findOneAndUpdate',
        Query.prototype._findOneAndUpdate, this, kareemOptions);
  }
}

Query.prototype.limit = function(limit) {
  this._limit = limit;
};

Query.prototype.count = function(conditions, callback) {
  if (typeof conditions === 'function') {
    callback = conditions;
    conditions = undefined;
  }
  if(conditions) {
    this.merge(conditions);
  }
  // if (mquery.canMerge(conditions)) {
  //   this.merge(conditions);
  // }

  this.op = 'count';
  if (callback) {
    this._count(callback);
  }

  return this;
};

Query.prototype._count = function(callback) {
  var _this = this;
  var cb = function(err, docs) {
    if (err) {
      return callback(err);
    }

    _this.__parseDataOneValue('count', docs, callback);
  };
  this._select(['count(*)'], this._conditions, 1, null, cb);
};

Query.prototype._execUpdate = function(callback) {
  var schema = this.model.schema;
  var doValidate;
  var _this;

  var castedQuery = this._conditions;
  var castedDoc = this._update;
  var options = this.options;

  if (this._castError) {
    callback(this._castError);
    return this;
  }

  if (this.options.runValidators) {
    _this = this;
    doValidate = updateValidators(this, schema, castedDoc, options);
    var _callback = function(err) {
      if (err) {
        return callback(err);
      }

      Query.base.update.call(_this, castedQuery, castedDoc, options, callback);
    };
    try {
      doValidate(_callback);
    } catch (err) {
      process.nextTick(function() {
        callback(err);
      });
    }
    return this;
  }

  Query.base.update.call(this, castedQuery, castedDoc, options, callback);
  return this;
};

Query.prototype.find = function(conditions, callback) {
  if (typeof conditions === 'function') {
    callback = conditions;
    conditions = {};
  }

  conditions = utils.toObject(conditions);
  if(conditions) {
    this.merge(conditions);
  }
  this.op = 'find';

  // if (mquery.canMerge(conditions)) {
  //   this.merge(conditions);
  // }

  // prepareDiscriminatorCriteria(this);

  // try {
  //   this.cast(this.model);
  //   this._castError = null;
  // } catch (err) {
  //   this._castError = err;
  // }

  // if we don't have a callback, then just return the query object
  if (callback) {
    this._find(callback);
  }

  return this;
};

Query.prototype._find = function(callback) {
  // if (this._castError) {
  //   callback(this._castError);
  //   return this;
  // }

  this._applyPaths();
  //this._fields = this._castFields(this._fields);

  // var fields = this._fieldsForExec();
  // var options = this._mongooseOptions;
  var _this = this;

  var fields = this._getFields();

  var cb = function(err, docs) {
    if (err) {
      return callback(err);
    }
    if (_this.options.populate) {
      var pop = utils.object.vals( _this.options.populate );
      _this.model.populate(docs, pop, function(err, docs) {
        if (err) {
          return callback(err);
        }
        _this.__parseData(fields, docs, callback);
      });
    } else {
      _this.__parseData(fields, docs, callback);
    }


    // if (docs.length === 0) {
    //   return callback(null, docs);
    // }

    // if (!options.populate) {
    //   return options.lean === true
    //       ? callback(null, docs)
    //       : completeMany(_this.model, docs, fields, _this, null, callback);
    // }

    // var pop = helpers.preparePopulationOptionsMQ(_this, options);
    // pop.__noPromise = true;
    // _this.model.populate(docs, pop, function(err, docs) {
    //   if (err) return callback(err);
    //   return options.lean === true
    //       ? callback(null, docs)
    //       : completeMany(_this.model, docs, fields, _this, pop, callback);
    // });
  };

  //console.log('fields', this._fields);



  this._select(fields, this._conditions, this._limit, this._sort, cb);

  return this;
};

Query.prototype._select = function(fields, conditions, limit, sort, cb) {
  var query = 'SELECT ', params = [];
  for(var i = 0; i < fields.length; i++) {
    if(i !== 0) {
      query += ', ';
    }
    query += fields[i];
  }
  query += ' FROM ' + this.model.modelName;
  if(conditions) {
    var where = '', path;
    for(var p in conditions) {
      path = this.schema.paths[p];
      if(path) {
        if(where.length > 0) {
          where += ' AND ';
        }

        if(utils.isObject(conditions[p])) {
          for(var op in conditions[p]) {
            if(OPERATIONS[op]) {
              where += path.options.name + ' IN (';
              for(var o = 0; o < conditions[p][op].length; o++) {
                if(o > 0) {
                  where += ', ';
                }
                where += '?';
                params.push(path.castForQuery( conditions[p][op][o] ) );
              }
              where += ')';
            }
          }
        } else {
          where += path.options.name + ' = ?';
          params.push(path.castForQuery( conditions[p]) );
        }
      }
    }

    if(where.length > 0) {
      query += ' WHERE ' + where;
    }
  }
  if(sort) {
    var column = sort.substring(1, sort.length);
    query += ' ORDER BY ' + column;
    if(sort[0] === '-') {
      query += ' DESC';
    } else {
      query += ' ASC';
    }
  }

  if(limit) {
    query += ' LIMIT ' + limit;
  }



  console.log('[cassandrom] ' + query);

  this.model.base.execute(query, params, {prepare: true}, cb);
};


Query.prototype.findOne = function(conditions, projection, options, callback) {
  if (typeof conditions === 'function') {
    callback = conditions;
    conditions = null;
    projection = null;
    options = null;
  } else if (typeof projection === 'function') {
    callback = projection;
    options = null;
    projection = null;
  } else if (typeof options === 'function') {
    callback = options;
    options = null;
  }

  // make sure we don't send in the whole Document to merge()
  console.log('findOne1', conditions);
  conditions = utils.toObject(conditions);

  this.op = 'findOne';
  if (options) {
    this.setOptions(options);
  }

  if (projection) {
    this.select(projection);
  }
  if(conditions) {
    this.merge(conditions);
  }

/*
  if (mquery.canMerge(conditions)) {
    this.merge(conditions);
  } else if (conditions != null) {
    throw new Error('Invalid argument to findOne(): ' +
      util.inspect(conditions));
  }
*/
  // prepareDiscriminatorCriteria(this);

  // try {
  //   this.cast(this.model);
  //   this._castError = null;
  // } catch (err) {
  //   this._castError = err;
  // }

  if (callback) {
    this._findOne(callback);
  }



  return this;
};

Query.prototype._findOne = function(callback) {
  if (this._castError) {
    return callback(this._castError);
  }

  this._applyPaths();
  // this._fields = this._castFields(this._fields);

  // var options = this._mongooseOptions;
  // var projection = this._fieldsForExec();
  var _this = this;
  var fields = this._getFields();

  var cb = function(err, docs) {
    if (err) {
      return callback(err);
    }

    //console.log('populate', _this.options.populate);

    if (_this.options.populate) {
      var pop = utils.object.vals( _this.options.populate );
      _this.model.populate(docs, pop, function(err, docs) {
        if (err) {
          return callback(err);
        }
        _this.__parseDataOne(fields, docs, callback);
      });
    } else {
      _this.__parseDataOne(fields, docs, callback);
    }
  };

console.log('findOne', this._conditions);

  this._select(fields, this._conditions, 1, this._sort, cb);
/*
  // don't pass in the conditions because we already merged them in
  Query.base.findOne.call(_this, {}, function(err, doc) {
    if (err) {
      return callback(err);
    }
    if (!doc) {
      return callback(null, null);
    }

    if (!options.populate) {
      return options.lean === true
          ? callback(null, doc)
          : completeOne(_this.model, doc, null, projection, _this, null, callback);
    }

    var pop = helpers.preparePopulationOptionsMQ(_this, options);
    pop.__noPromise = true;
    _this.model.populate(doc, pop, function(err, doc) {
      if (err) {
        return callback(err);
      }
      return options.lean === true
          ? callback(null, doc)
          : completeOne(_this.model, doc, null, projection, _this, pop, callback);
    });
  });
*/
};

Query.prototype.findOneAndRemove = function(conditions, options, callback) {
  this.op = 'findOneAndRemove';
  this._validate();

  switch (arguments.length) {
    case 2:
      if (typeof options === 'function') {
        callback = options;
        options = {};
      }
      break;
    case 1:
      if (typeof conditions === 'function') {
        callback = conditions;
        conditions = undefined;
        options = undefined;
      }
      break;
  }

  // if (mquery.canMerge(conditions)) {
  //   this.merge(conditions);
  // }

  options && this.setOptions(options);

  if (!callback) {
    return this;
  }

  this._findOneAndRemove(callback);

  return this;
};

Query.prototype._findOneAndRemove = function(callback) {
  Query.base.findOneAndRemove.call(this, callback);
};

Query.prototype.findOneAndUpdate = function(criteria, doc, options, callback) {
  this.op = 'findOneAndUpdate';
  this._validate();

  switch (arguments.length) {
    case 3:
      if (typeof options === 'function') {
        callback = options;
        options = {};
      }
      break;
    case 2:
      if (typeof doc === 'function') {
        callback = doc;
        doc = criteria;
        criteria = undefined;
      }
      options = undefined;
      break;
    case 1:
      if (typeof criteria === 'function') {
        callback = criteria;
        criteria = options = doc = undefined;
      } else {
        doc = criteria;
        criteria = options = undefined;
      }
  }

  if(this._conditions) {
    this.merge(criteria);
  }

  // apply doc
  if (doc) {
    this._mergeUpdate(doc);
  }

  if (options) {
    options = utils.clone(options, { retainKeyOrder: true });
    if (options.projection) {
      this.select(options.projection);
      delete options.projection;
    }
    if (options.fields) {
      this.select(options.fields);
      delete options.fields;
    }

    this.setOptions(options);
  }

  if (!callback) {
    return this;
  }

  return this._findOneAndUpdate(callback);
};

Query.prototype._findOneAndUpdate = function(callback) {
  this._findAndModify('update', callback);
  return this;
};

Query.prototype.changePrimaryKey = function(source) {
  if('_id' in source) {
    var id = source._id;
    delete source._id;
    source.id = id;
  }
};

Query.prototype.merge = function(source) {
  if (!source) {
    this.changePrimaryKey(this._conditions);
    return this;
  }

  var opts = { retainKeyOrder: this.options.retainKeyOrder, overwrite: true };

  if (source instanceof Query) {
    // if source has a feature, apply it to ourselves

    if (source._conditions) {
      utils.merge(this._conditions, source._conditions, opts);
    }

    if (source._fields) {
      this._fields || (this._fields = {});
      utils.merge(this._fields, source._fields, opts);
    }

    if (source.options) {
      this.options || (this.options = {});
      utils.merge(this.options, source.options, opts);
    }

    if (source._update) {
      this._update || (this._update = {});
      utils.mergeClone(this._update, source._update);
    }

    if (source._distinct) {
      this._distinct = source._distinct;
    }
    this.changePrimaryKey(this._conditions);
    return this;
  }

  // plain object
  utils.merge(this._conditions, source, opts);
  this.changePrimaryKey(this._conditions);

  return this;
};

// Query.prototype.cast = function(model, obj) {
//   obj || (obj = this._conditions);

//   try {
//     return cast(model.schema, obj, {
//       upsert: this.options && this.options.upsert,
//       strict: (this.options && this.options.strict) ||
//         (model.schema.options && model.schema.options.strict)
//     });
//   } catch (err) {
//     // CastError, assign model
//     if (typeof err.setModel === 'function') {
//       err.setModel(model);
//     }
//     throw err;
//   }
// };

Query.prototype._getFields = function() {
  if(this._fields) {
    //console.log('-- ' + this._fields);
    return this._fields;
  } else {
    var selected = [],
      seen = [];

    var analyzeSchema = function(schema, prefix) {
      prefix || (prefix = '');

      // avoid recursion
      if (~seen.indexOf(schema)) return;
      seen.push(schema);

      schema.eachPath(function(path, type) {
        if(path[0] !== '_') {
          if (prefix) {
            path = prefix + '_' + path;
          }
          // analyzePath(path, type);
          selected.push(path.replace('.', '_'));

          // array of subdocs?
          if (type.schema) {
            analyzeSchema(type.schema, path);
          }
        }
      });
    };

    analyzeSchema(this.model.schema);

    return selected;
  }
};

Query.prototype._applyPaths = function applyPaths() {
  var fields = this._fields,
      exclude,
      keys,
      ki;

  if (fields) {
    keys = Object.keys(fields);
    ki = keys.length;

    while (ki--) {
      if (keys[ki][0] === '+') continue;
      exclude = fields[keys[ki]] === 0;
      break;
    }
  }

  var selected = [],
      excluded = [],
      seen = [];

  var analyzePath = function(path, type) {
    if (typeof type.selected !== 'boolean') return;

    var plusPath = '+' + path;
    if (fields && plusPath in fields) {
      // forced inclusion
      delete fields[plusPath];

      // if there are other fields being included, add this one
      // if no other included fields, leave this out (implied inclusion)
      if (exclude === false && keys.length > 1 && !~keys.indexOf(path)) {
        fields[path] = 1;
      }

      return;
    }

    // check for parent exclusions
    var root = path.split('.')[0];
    if (~excluded.indexOf(root)) return;

    (type.selected ? selected : excluded).push(path);
  };

  var analyzeSchema = function(schema, prefix) {
    prefix || (prefix = '');

    // avoid recursion
    if (~seen.indexOf(schema)) return;
    seen.push(schema);

    schema.eachPath(function(path, type) {
      if (prefix) path = prefix + '.' + path;
      analyzePath(path, type);

      // array of subdocs?
      if (type.schema) {
        analyzeSchema(type.schema, path);
      }
    });
  };

  analyzeSchema(this.model.schema);
};

Query.prototype.select = function(fields) {
  this._fields = fields;
}

Query.prototype.populate = function() {
  console.log('###');
  var res = utils.populate.apply(null, arguments);
  if(!this.options.populate) {
    this.options.populate = {};
  }
  var pop = this.options.populate;

  console.log('pppp ', arguments, res, pop);

  for (var i = 0; i < res.length; ++i) {
    var path = res[i].path;
    if (pop[path] && pop[path].populate && res[i].populate) {
      res[i].populate = pop[path].populate.concat(res[i].populate);
    }
    pop[res[i].path] = res[i];
  }


  return this;
};


Query.prototype.skip = function(skip) {
  // console.log('skip ' + skip);
  this._skip = skip;
  return this;
};

Query.prototype.sort = function(name) {
  // this._sort = name;
  return this;
};

Query.prototype.where = function(conditions) {
  if (utils.isObject(conditions)) {
    this.merge(conditions);
  } else {
    if(arguments.length === 2) {
      this._conditions[arguments[0]] = arguments[1];
    } else {
      console.log('TODO where');
    }
  }
  return this;
};

Query.prototype.exec = function exec(op, callback) {
  var _this = this;

  if (typeof op === 'function') {
    callback = op;
    op = null;
  } else if (typeof op === 'string') {
    this.op = op;
  }

  var _results;
  var promise = new Promise(function(resolve, reject) {
    if (!_this.op) {
      resolve();
      return;
    }

    _this[_this.op].call(_this, function(error, res) {
      if (error) {
        reject(error);
        return;
      }
      _results = arguments;
      resolve(res);
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
        // If we made it here, we must have an error in the callback re:
        // gh-4500, so we need to emit.
        setImmediate(function() {
          _this.model.emit('error', error);
        });
      });
  }

  return promise;
};

Query.prototype.getQuery = function() {
  return this._conditions;
};

Query.prototype.setOptions = function(options, overwrite) {
  if(options) {
    console.log('set options ', options);
    this.options = options;
  } else {
    this.options = {};
  }
};

Query.prototype.__parseData = function(fields, docs, callback) {
  var list = [];
  if(docs.rows && docs.rows.length > 0) {
    var size = docs.rows.length - 1, count = docs.rows.length;
    for(; size >= 0; size--) {
      obj = new this.model(undefined, fields, true);
      list.push( obj );
      obj.init(docs.rows[size], undefined, function(err) {
        --count;
        if (err) {
          return callback(err);
        }
        if(count === 0) {
          callback(null, list);
        }
      });
    }
  } else {
    callback(null, list);
  }
  return list;
};

Query.prototype.__parseDataOneValue = function(field, docs, callback) {
  var one = null;
  if(docs.rows && docs.rows.length > 0) {
    callback(null, docs.rows[0][field]);
  } else {
    callback(null, one);
  }
  return one;
};

Query.prototype.__parseDataOne = function(fields, docs, callback) {
  var one = null;
  if(docs.rows && docs.rows.length > 0) {
    one = new this.model(undefined, fields, true);
    one.init(docs.rows[0], undefined, function(err) {
      if (err) {
        return callback(err);
      }
      callback(null, one);
    });
  } else {
    callback(null, one);
  }
  return one;
};

module.exports = Query;