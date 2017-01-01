
var hooks = require('hooks-fixed');
var utils = require('./utils');
var error = require('./error');
var Promise = require('./promise');
var InternalCache = require('./internal');
var EventEmitter = require('events').EventEmitter;
var deepEqual = utils.deepEqual;
var clone = utils.clone;

function Document(obj, fields) {
  this.$__ = new InternalCache;
  this.$__.emitter = new EventEmitter();

  this.isNew = true;
  this.errors = undefined;

  var schema = this.schema;

  var required = schema.requiredPaths(true);
  for (var i = 0; i < required.length; ++i) {
    this.$__.activePaths.require(required[i]);
  }
  this.$__.emitter.setMaxListeners(0);

  //obj = this.$__normalize(obj);
  this._doc = this.$__buildDoc(obj, fields);

  if (obj) {
    if (obj instanceof Document) {
      this.isNew = obj.isNew;
    }
    this.set(obj, undefined, true);
  }

  var _this = this,
        keys = Object.keys(this._doc);

  keys.forEach(function(key) {
    if (!(key in schema.tree)) {
      defineKey(key, null, _this);
    }
  });

  this.$__registerHooksFromSchema();
}


utils.each(
  ['on', 'once', 'emit', 'listeners', 'removeListener', 'setMaxListeners',
    'removeAllListeners', 'addListener'],
  function(emitterFn) {
    Document.prototype[emitterFn] = function() {
      return this.$__.emitter[emitterFn].apply(this.$__.emitter, arguments);
    };
});

/*!
 * Set up middleware support
 */

for (var k in hooks) {
  if (k === 'pre' || k === 'post') {
    Document.prototype['$' + k] = Document['$' + k] = hooks[k];
  } else {
    Document.prototype[k] = Document[k] = hooks[k];
  }
}

Document.prototype.constructor = Document;

Document.prototype.schema;

Document.prototype.isNew;

Document.prototype.id;

Document.prototype.errors;

Document.prototype.init = function(doc, opts, fn) {

  // do not prefix this method with $__ since its
  // used by public hooks

  if (typeof opts === 'function') {
    fn = opts;
    opts = null;
  }

  this.isNew = false;

  // handle docs with populated paths
  // If doc._id is not null or undefined
  if (doc._id !== null && doc._id !== undefined &&
    opts && opts.populated && opts.populated.length) {
    var id = String(doc._id);
    for (var i = 0; i < opts.populated.length; ++i) {
      var item = opts.populated[i];
      if (item.isVirtual) {
        this.populated(item.path, utils.getValue(item.path, doc), item);
      } else {
        this.populated(item.path, item._docs[id], item);
      }
    }
  }

  init(this, doc, this._doc);
  this.$__storeShard();

  this.emit('init', this);
  if (fn) {
    fn(null);
  }
  return this;
};

function init(self, obj, doc, prefix) {
  prefix = prefix || '';

  var keys = Object.keys(obj);
  var len = keys.length;
  var schema;
  var path;
  var i;
  var index = 0;

  if (self.schema.options.retainKeyOrder) {
    while (index < len) {
      _init(index++);
    }
  } else {
    while (len--) {
      _init(len);
    }
  }

  function _init(index) {
    i = keys[index];
    path = prefix + i;
    schema = self.schema.path(path);

    if (!schema && utils.isObject(obj[i]) &&
        (!obj[i].constructor || obj[i].constructor.name === 'Object')) {
      // assume nested object
      if (!doc[i]) {
        doc[i] = {};
      }
      init(self, obj[i], doc[i], path + '.');
    } else {
      if (obj[i] === null) {
        doc[i] = null;
      } else if (obj[i] !== undefined) {
        if (schema) {
          try {
            doc[i] = schema.cast(obj[i], self, true);
          } catch (e) {
            self.invalidate(e.path, new ValidatorError({
              path: e.path,
              message: e.message,
              type: 'cast',
              value: e.value
            }));
          }
        } else {
          doc[i] = obj[i];
        }
      }
      // mark as hydrated
      if (!self.isModified(path)) {
        self.$__.activePaths.init(path);
      }
    }
  }
};

/**
 * Gets a raw value from a path (no getters)
 *
 * @param {String} path
 * @api private
 */

Document.prototype.getValue = function(path) {
  return utils.getValue(path, this._doc);
};

/**
 * Sets a raw value for a path (no casting, setters, transformations)
 *
 * @param {String} path
 * @param {Object} value
 * @api private
 */

Document.prototype.setValue = function(path, val) {
  utils.setValue(path, val, this._doc);
  return this;
};


Document.prototype.isModified = function(paths) {
  if (paths) {
    if (!Array.isArray(paths)) {
      paths = paths.split(' ');
    }
    var modified = this.modifiedPaths();

    var directModifiedPaths = Object.keys(this.$__.activePaths.states.modify);
    var isModifiedChild = paths.some(function(path) {
      return !!~modified.indexOf(path);
    });
    return isModifiedChild || paths.some(function(path) {
      return directModifiedPaths.some(function(mod) {
        return mod === path || path.indexOf(mod + '.') === 0;
      });
    });
  }
  return this.$__.activePaths.some('modify');
};

/**
 * Determine if we should mark this change as modified.
 *
 * @return {Boolean}
 * @api private
 * @method $__shouldModify
 * @memberOf Document
 */

Document.prototype.$__shouldModify = function(pathToMark, path, constructing, parts, schema, val, priorVal) {
  if (this.isNew) {
    return true;
  }

  if (undefined === val && !this.isSelected(path)) {
    // when a path is not selected in a query, its initial
    // value will be undefined.
    return true;
  }

  if (undefined === val && path in this.$__.activePaths.states.default) {
    // we're just unsetting the default value which was never saved
    return false;
  }

  // gh-3992: if setting a populated field to a doc, don't mark modified
  // if they have the same _id
  if (this.populated(path) &&
      val instanceof Document &&
      deepEqual(val._id, priorVal)) {
    return false;
  }

  if (!deepEqual(val, priorVal || this.get(path))) {
    return true;
  }

  if (!constructing &&
      val !== null &&
      val !== undefined &&
      path in this.$__.activePaths.states.default &&
      deepEqual(val, schema.getDefault(this, constructing))) {
    // a path with a default was $unset on the server
    // and the user is setting it to the same value again
    return true;
  }
  return false;
};

Document.prototype.modifiedPaths = function() {
  var directModifiedPaths = Object.keys(this.$__.activePaths.states.modify);
  return directModifiedPaths.reduce(function(list, path) {
    var parts = path.split('.');
    return list.concat(parts.reduce(function(chains, part, i) {
      return chains.concat(parts.slice(0, i).concat(part).join('.'));
    }, []).filter(function(chain) {
      return (list.indexOf(chain) === -1);
    }));
  }, []);
};

/*
Document.prototype.$__normalize = function(obj) {
  var paths = Object.keys(this.schema.paths),
  old;
  for(var i = 0; i < paths.length; i++) {
    var p = paths[i];
    var type = this.schema.path(p);
    var name = type.options.name;
    //console.log('*= ' + p + '   ' + name);
    if(p !== name && obj[name]) {
      old = obj[name];
      delete obj[name];
      obj[p] = old;
    }
  }
  return obj;
};
*/

Document.prototype.$__buildDoc = function (obj, fields) {
  var doc = {}
    , self = this
    , exclude
    , keys
    , key
    , ki;

  if (fields && 'Object' === fields.constructor.name) {
    keys = Object.keys(fields);
    ki = keys.length;

    // while (ki--) {
    //   if ('_id' !== keys[ki]) {
    //     exclude = 0 === fields[keys[ki]];
    //     break;
    //   }
    // }
  }

  var paths = Object.keys(this.schema.paths)
    , plen = paths.length
    , ii = 0
  var included = false;

  for (; ii < plen; ++ii) {
    var p = paths[ii];

    // if ('_id' == p) {
    //   if (skipId) continue;
    //   if (obj && '_id' in obj) continue;
    // }

    var type = this.schema.paths[p]
      , path = p.split('.')
      , len = path.length
      , last = len-1
      , curPath = ''
      , doc_ = doc
      , i = 0

    for (; i < len; ++i) {
      var piece = path[i]
        , def;

      // support excluding intermediary levels
      // if (exclude) {
      //   curPath += piece;
      //   if (curPath in fields) break;
      //   curPath += '.';
      // }

      if (i === last) {
        if (fields && exclude !== null) {
          if (exclude === true) {
            // apply defaults to all non-excluded fields
            if (p in fields) {
              continue;
            }

            def = type.getDefault(self, false);
            if (typeof def !== 'undefined') {
              doc_[piece] = def;
              self.$__.activePaths.default(p);
            }
          } else if (included) {
            // selected field
            def = type.getDefault(self, false);
            if (typeof def !== 'undefined') {
              doc_[piece] = def;
              self.$__.activePaths.default(p);
            }
          }
        } else {
          def = type.getDefault(self, false);
          if (typeof def !== 'undefined') {
            doc_[piece] = def;
            self.$__.activePaths.default(p);
          }
        }
      } else {
        doc_ = doc_[piece] || (doc_[piece] = {});
      }
    }
  };

  return doc;
};

Document.prototype.markModified = function(path) {
  this.$__.activePaths.modify(path);
};

Document.prototype.unmarkModified = function(path) {
  this.$__.activePaths.init(path);
};

Document.prototype.$ignore = function(path) {
  this.$__.activePaths.ignore(path);
};

Document.prototype.$isDefault = function(path) {
  return (path in this.$__.activePaths.states.default);
};

Document.prototype.isDirectModified = function(path) {
  return (path in this.$__.activePaths.states.modify);
};

Document.prototype.isInit = function(path) {
  return (path in this.$__.activePaths.states.init);
};

/**
 * Handles the actual setting of the value and marking the path modified if appropriate.
 *
 * @api private
 * @method $__set
 * @memberOf Document
 */

Document.prototype.$__set = function(pathToMark, path, constructing, parts, schema, val, priorVal) {
  // Embedded = Embedded || require('./types/embedded');

  var shouldModify = this.$__shouldModify(pathToMark, path, constructing, parts,
    schema, val, priorVal);
  var _this = this;

  if (shouldModify) {
    this.markModified(pathToMark, val);

    // handle directly setting arrays (gh-1126)
    // MongooseArray || (MongooseArray = require('./types/array'));
    // if (val && val.isMongooseArray) {
    //   val._registerAtomic('$set', val);

    //   // Small hack for gh-1638: if we're overwriting the entire array, ignore
    //   // paths that were modified before the array overwrite
    //   this.$__.activePaths.forEach(function(modifiedPath) {
    //     if (modifiedPath.indexOf(path + '.') === 0) {
    //       _this.$__.activePaths.ignore(modifiedPath);
    //     }
    //   });
    // }
  }

  var obj = this._doc;
  var i = 0;
  var l = parts.length;
  var cur = '';

  for (; i < l; i++) {
    var next = i + 1;
    var last = next === l;
    cur += (cur ? '.' + parts[i] : parts[i]);

    if (last) {
      obj[parts[i]] = val;
    } else {
      if (obj[parts[i]] && obj[parts[i]].constructor.name === 'Object') {
        obj = obj[parts[i]];
      } else if (obj[parts[i]] && obj[parts[i]] instanceof Embedded) {
        obj = obj[parts[i]];
      } else if (obj[parts[i]] && obj[parts[i]].$isSingleNested) {
        obj = obj[parts[i]];
      } else if (obj[parts[i]] && Array.isArray(obj[parts[i]])) {
        obj = obj[parts[i]];
      } else {
        this.set(cur, {});
        obj = obj[parts[i]];
      }
    }
  }
};

Document.prototype.set = function (path, val, type, options) {
  if (type && 'Object' == type.constructor.name) {
    options = type;
    type = undefined;
  }

  var merge = options && options.merge
   , adhoc = type && true !== type
    , constructing = true === type
    , adhocs;

  if (adhoc) {
    adhocs = this.$__.adhocPaths || (this.$__.adhocPaths = {});
    adhocs[path] = Schema.interpretAsType(path, type);
  }

  if ('string' !== typeof path) {
    if (null === path || undefined === path) {
      var _ = path;
      path = val;
      val = _;

    } else {
      var prefix = val
        ? val + '.'
        : '';

      if (path instanceof Document) {
        if(path.$__isNested) {

        } else {
          path = path._doc;
        }
      }

      var keys = Object.keys(path)
        , len = keys.length
        // , pathtype
        , key;

      while( len > 0 ) {
        len--;

        key = keys[len];

        this.__process(len, path, key, prefix, constructing);
      }
      return this;
    }
  }

  var pathType = this.schema.pathType(path);
  if (pathType === 'nested' && val) {
    if (utils.isObject(val) &&
        (!val.constructor || val.constructor.name === 'Object')) {
      if (!merge) {
        this.setValue(path, null);
        cleanModifiedSubpaths(this, path);
      }

      if (Object.keys(val).length === 0) {
        this.setValue(path, {});
        this.markModified(path);
        cleanModifiedSubpaths(this, path);
      } else {
        this.set(val, path, constructing);
      }
      return this;
    }
    this.invalidate(path, new MongooseError.CastError('Object', val, path));
    return this;
  }


  var schema;
  var parts = path.split('.');

  // console.log('define ' + path + '  ' + pathType);
  if (pathType === 'adhocOrUndefined') {
    var mixed;

    for (i = 0; i < parts.length; ++i) {
      var subpath = parts.slice(0, i + 1).join('.');
      schema = this.schema.path(subpath);
      // if (schema instanceof MixedSchema) {
      //   // allow changes to sub paths of mixed types
      //   mixed = true;
      //   break;
      // }

      // If path is underneath a virtual, bypass everything and just set it.
      if (i + 1 < parts.length && this.schema.pathType(subpath) === 'virtual') {
        mpath.set(path, val, this);
        return this;
      }
    }
  } else if (pathType === 'virtual') {


    schema = this.schema.virtualpath(path);
    schema.applySetters(val, this);
    return this;
  } else {
    schema = this.schema.path(path);
  }

  var pathToMark;

  // When using the $set operator the path to the field must already exist.
  // Else mongodb throws: "LEFT_SUBFIELD only supports Object"

  if (parts.length <= 1) {
    pathToMark = path;
  } else {
    for (var i = 0; i < parts.length; ++i) {
      var subpath = parts.slice(0, i+1).join('.');
      // console.log('sub ' + subpath  + '  ' + schema  + '  ' + val);

      if ( subpath in this.schema.paths
          || this.get(subpath) === null) {
        pathToMark = subpath;
        break;
      }
    }

    if (!pathToMark) {
      pathToMark = path;
    }
  }

  // if this doc is being constructed we should not trigger getters
  var priorVal = constructing
    ? undefined
    : this.getValue(path);

  if (!schema || undefined === val) {
    this.$__set(pathToMark, path, constructing, parts, schema, val, priorVal);
    return this;
  }

// console.log('set ' + path + ': ' + val + ', ' + priorVal, schema);
  var self = this;
  var shouldSet = this.$__try(function(){
    val = schema.applySetters(val, self, false, priorVal);
  });

  if (shouldSet) {
    this.$__set(pathToMark, path, constructing, parts, schema, val, priorVal);
  }

  return this;
};

Document.prototype.__process = function(i, path, key, prefix, constructing) {
  var pathName = prefix + key;
  var pathtype = this.schema.pathType(pathName);
  if (path[key] !== null
        && path[key] !== void 0
          // need to know if plain object - no Buffer, ObjectId, ref, etc
        && utils.isObject(path[key])
        && (!path[key].constructor || path[key].constructor.name === 'Object')
        && pathtype !== 'virtual'
        && pathtype !== 'real'
       // && !(this.$__path(pathName) instanceof MixedSchema)
        && !(this.schema.paths[pathName] &&
        this.schema.paths[pathName].options &&
        this.schema.paths[pathName].options.ref)) {
      this.set(path[key], prefix + key, constructing);
  }
  else /* if (strict) */ {
      // Don't overwrite defaults with undefined keys (gh-3981)
      if (constructing && path[key] === void 0 &&
          this.get(key) !== void 0) {
        return;
      }

      if (pathtype === 'real' || pathtype === 'virtual') {
        // Check for setting single embedded schema to document (gh-3535)
        var p = path[key];
        if (this.schema.paths[pathName] &&
            this.schema.paths[pathName].$isSingleNested &&
            path[key] instanceof Document) {
          console.log('process ' + p);
          p = p.toObject({ virtuals: false, transform: false });
        }
        this.set(prefix + key, p, constructing);
      } else if (pathtype === 'nested' && path[key] instanceof Document) {
        console.log('process ' + path[key]);

        this.set(prefix + key,
            path[key].toObject({transform: false}), constructing);
      }
/*
      else if (strict === 'throw') {
        if (pathtype === 'nested') {
          throw new ObjectExpectedError(key, path[key]);
        } else {
          throw new StrictModeError(key);
        }
      }
*/
  }
  // else if (path[key] !== void 0) {
  //     this.set(prefix + key, path[key], constructing);
  // }
};

Document.prototype.$__registerHooksFromSchema = function() {
  var _this = this;
  var q = _this.schema && _this.schema.callQueue;
  if (!q.length) {
    return _this;
  }

  // we are only interested in 'pre' hooks, and group by point-cut
  var toWrap = q.reduce(function(seed, pair) {
    if (pair[0] !== 'pre' && pair[0] !== 'post' && pair[0] !== 'on') {
      _this[pair[0]].apply(_this, pair[1]);
      return seed;
    }
    var args = [].slice.call(pair[1]);
    var pointCut = pair[0] === 'on' ? 'post' : args[0];
    if (!(pointCut in seed)) {
      seed[pointCut] = {post: [], pre: []};
    }
    if (pair[0] === 'post') {
      seed[pointCut].post.push(args);
    } else if (pair[0] === 'on') {
      seed[pointCut].push(args);
    } else {
      seed[pointCut].pre.push(args);
    }
    return seed;
  }, {post: []});

  // 'post' hooks are simpler
  toWrap.post.forEach(function(args) {
    _this.on.apply(_this, args);
  });
  delete toWrap.post;

  // 'init' should be synchronous on subdocuments
  /*
  if (toWrap.init && _this instanceof Embedded) {
    if (toWrap.init.pre) {
      toWrap.init.pre.forEach(function(args) {
        _this.$pre.apply(_this, args);
      });
    }
    if (toWrap.init.post) {
      toWrap.init.post.forEach(function(args) {
        _this.$post.apply(_this, args);
      });
    }
    delete toWrap.init;
  } else */
  if (toWrap.set) {
    // Set hooks also need to be sync re: gh-3479
    if (toWrap.set.pre) {
      toWrap.set.pre.forEach(function(args) {
        _this.$pre.apply(_this, args);
      });
    }
    if (toWrap.set.post) {
      toWrap.set.post.forEach(function(args) {
        _this.$post.apply(_this, args);
      });
    }
    delete toWrap.set;
  }

  Object.keys(toWrap).forEach(function(pointCut) {
    // this is so we can wrap everything into a promise;
    var newName = ('$__original_' + pointCut);
    if (!_this[pointCut]) {
      return;
    }
    _this[newName] = _this[pointCut];
    _this[pointCut] = function wrappedPointCut() {
      var args = [].slice.call(arguments);
      var lastArg = args.pop();
      var fn;
      var originalError = new Error();
      var $results;
      if (lastArg && typeof lastArg !== 'function') {
        args.push(lastArg);
      } else {
        fn = lastArg;
      }

      var promise = new Promise(function(resolve, reject) {
        args.push(function(error) {
          if (error) {
            // gh-2633: since VersionError is very generic, take the
            // stack trace of the original save() function call rather
            // than the async trace
            // if (error instanceof VersionError) {
            //   error.stack = originalError.stack;
            // }
            _this.$__handleReject(error);
            reject(error);
            return;
          }

          // There may be multiple results and promise libs other than
          // mpromise don't support passing multiple values to `resolve()`
          $results = Array.prototype.slice.call(arguments, 1);
          resolve.apply(promise, $results);
        });

console.log('ups ' + newName, args);

        _this[newName].apply(_this, args);
      });
      if (fn) {
        if (_this.constructor.$wrapCallback) {
          fn = _this.constructor.$wrapCallback(fn);
        }
        return promise.then(
          function() {
            process.nextTick(function() {
              fn.apply(null, [null].concat($results));
            });
          },
          function(error) {
            process.nextTick(function() {
              fn(error);
            });
          });
      }
      return promise;
    };

    toWrap[pointCut].pre.forEach(function(args) {
      args[0] = newName;
      _this.$pre.apply(_this, args);
    });
    toWrap[pointCut].post.forEach(function(args) {
      args[0] = newName;
      _this.$post.apply(_this, args);
    });

  });

  return _this;
};

Document.prototype.populated = function(path, val, options) {
  // val and options are internal

  if (val === null || val === void 0) {
    if (!this.$__.populated) {
      return undefined;
    }
    var v = this.$__.populated[path];
    if (v) {
      return v.value;
    }
    return undefined;
  }

  // internal

  if (val === true) {
    if (!this.$__.populated) {
      return undefined;
    }
    return this.$__.populated[path];
  }

  this.$__.populated || (this.$__.populated = {});
  this.$__.populated[path] = {value: val, options: options};
  return val;
};


Document.prototype.$__path = function(path) {
  var adhocs = this.$__.adhocPaths,
      adhocType = adhocs && adhocs[path];

  if (adhocType) {
    return adhocType;
  }
  return this.schema.path(path);
};

Document.prototype.get = function (path, type) {
  var adhoc;
  if (type) {
    adhoc = Schema.interpretAsType(path, type, this.schema.options);
  }

  var schema = this.$__path(path) || this.schema.virtualpath(path),
      pieces = path.split('.'),
      obj = this._doc;

  for (var i = 0, l = pieces.length; i < l; i++) {
    obj = obj === null || obj === void 0
        ? undefined
        : obj[pieces[i]];
  }

  if (adhoc) {
    obj = adhoc.cast(obj);
  }

  // Check if this path is populated - don't apply getters if it is,
  // because otherwise its a nested object. See gh-3357
  if (schema && !this.populated(path)) {
    obj = schema.applyGetters(obj, this);
  }

  return obj;
};


function defineKey(prop, subprops, prototype, prefix, keys, options) {
  var path = (prefix ? prefix + '.' : '') + prop;
  prefix = prefix || '';

  if (subprops) {
    Object.defineProperty(prototype, prop, {
      enumerable: true,
      configurable: true,
      get: function() {
        var _this = this;
        if (!this.$__.getters) {
          this.$__.getters = {};
        }

        if (!this.$__.getters[path]) {
          var nested = Object.create(Object.getPrototypeOf(this), getOwnPropertyDescriptors(this));

          // save scope for nested getters/setters
          if (!prefix) {
            nested.$__.scope = this;
          }

          // shadow inherited getters from sub-objects so
          // thing.nested.nested.nested... doesn't occur (gh-366)
          var i = 0,
              len = keys.length;

          for (; i < len; ++i) {
            // over-write the parents getter without triggering it
            Object.defineProperty(nested, keys[i], {
              enumerable: false,    // It doesn't show up.
              writable: true,       // We can set it later.
              configurable: true,   // We can Object.defineProperty again.
              value: undefined      // It shadows its parent.
            });
          }

          Object.defineProperty(nested, 'toObject', {
            enumerable: true,
            configurable: true,
            writable: false,
            value: function() {
              return _this.get(path);
            }
          });

          Object.defineProperty(nested, 'toJSON', {
            enumerable: true,
            configurable: true,
            writable: false,
            value: function() {
              return _this.get(path);
            }
          });

          Object.defineProperty(nested, '$__isNested', {
            enumerable: true,
            configurable: true,
            writable: false,
            value: true
          });

          compile(subprops, nested, path, options);
          this.$__.getters[path] = nested;
        }

        return this.$__.getters[path];
      },
      set: function(v) {
        if (v instanceof Document) {
          v = v.toObject({ transform: false });
        }
        return (this.$__.scope || this).set(path, v);
      }
    });
  } else {
    Object.defineProperty(prototype, prop, {
      enumerable: true,
      configurable: true,
      get: function() {
        return this.get.call(this.$__.scope || this, path);
      },
      set: function(v) {
        return this.set.call(this.$__.scope || this, path, v);
      }
    });
  }
}

Document.prototype.$__getAllSubdocs = function() {
  // DocumentArray || (DocumentArray = require('./types/documentarray'));
  // Embedded = Embedded || require('./types/embedded');

  function docReducer(seed, path) {
    var val = this[path];

    // if (val instanceof Embedded) {
    //   seed.push(val);
    // }
    if (val && val.$isSingleNested) {
      seed = Object.keys(val._doc).reduce(docReducer.bind(val._doc), seed);
      seed.push(val);
    }
    /*
    if (val && val.isMongooseDocumentArray) {
      val.forEach(function _docReduce(doc) {
        if (!doc || !doc._doc) {
          return;
        }
        // if (doc instanceof Embedded) {
        //   seed.push(doc);
        // }
        seed = Object.keys(doc._doc).reduce(docReducer.bind(doc._doc), seed);
      });
    } else
    */
    if (val instanceof Document && val.$__isNested) {
      val = val.toObject();
      if (val) {
        seed = Object.keys(val).reduce(docReducer.bind(val), seed);
      }
    }
    return seed;
  }

  var subDocs = Object.keys(this._doc).reduce(docReducer.bind(this), []);

  return subDocs;
};

Document.prototype.$__storeShard = function() {
  // backwards compat
  var key = this.schema.options.shardKey || this.schema.options.shardkey;
  if (!(key && key.constructor.name === 'Object')) {
    return;
  }

  var orig = this.$__.shardval = {},
      paths = Object.keys(key),
      len = paths.length,
      val;

  for (var i = 0; i < len; ++i) {
    val = this.getValue(paths[i]);
    // if (isMongooseObject(val)) {
    //   orig[paths[i]] = val.toObject({depopulate: true, _isNested: true});
    // } else
    if (val !== null && val !== undefined && val.valueOf &&
          // Explicitly don't take value of dates
        (!val.constructor || val.constructor.name !== 'Date')) {
      orig[paths[i]] = val.valueOf();
    } else {
      orig[paths[i]] = val;
    }
  }
};

Document.prototype.$__handleReject = function handleReject(err) {
  // emit on the Model if listening
  if (this.listeners('error').length) {
    this.emit('error', err);
  } else if (this.constructor.listeners && this.constructor.listeners('error').length) {
    this.constructor.emit('error', err);
  } else if (this.listeners && this.listeners('error').length) {
    this.emit('error', err);
  }
};

Document.prototype.$__reset = function reset() {
  var _this = this;
 // DocumentArray || (DocumentArray = require('./types/documentarray'));

  this.$__.activePaths
  .map('init', 'modify', function(i) {
    return _this.getValue(i);
  })
  .filter(function(val) {
    return val && val instanceof Array /* && val.isMongooseDocumentArray */ && val.length;
  })
  .forEach(function(array) {
    var i = array.length;
    while (i--) {
      var doc = array[i];
      if (!doc) {
        continue;
      }
      doc.$__reset();
    }
  });

  // clear atomics
  this.$__dirty().forEach(function(dirt) {
    var type = dirt.value;
    if (type && type._atomics) {
      type._atomics = {};
    }
  });

  // Clear 'dirty' cache
  this.$__.activePaths.clear('modify');
  this.$__.activePaths.clear('default');
  this.$__.validationError = undefined;
  this.errors = undefined;
  _this = this;
  this.schema.requiredPaths().forEach(function(path) {
    _this.$__.activePaths.require(path);
  });

  return this;
};

/**
 * Returns this documents dirty paths / vals.
 *
 * @api private
 * @method $__dirty
 * @memberOf Document
 */

Document.prototype.$__dirty = function() {
  var _this = this;

  var all = this.$__.activePaths.map('modify', function(path) {
    return {
      path: path,
      value: _this.getValue(path),
      schema: _this.$__path(path)
    };
  });

  // gh-2558: if we had to set a default and the value is not undefined,
  // we have to save as well
  all = all.concat(this.$__.activePaths.map('default', function(path) {
    if (/*path === '_id' || */ !_this.getValue(path)) {
      return;
    }
    return {
      path: path,
      value: _this.getValue(path),
      schema: _this.$__path(path)
    };
  }));

  // Sort dirty paths in a flat hierarchy.
  all.sort(function(a, b) {
    return (a.path < b.path ? -1 : (a.path > b.path ? 1 : 0));
  });

  // Ignore "foo.a" if "foo" is dirty already.
  var minimal = [],
      lastPath,
      top;

  all.forEach(function(item) {
    if (!item) {
      return;
    }
    if (item.path.indexOf(lastPath) !== 0) {
      lastPath = item.path + '.';
      minimal.push(item);
      top = item;
    } else {
      // special case for top level MongooseArrays
      if (top.value && top.value._atomics && top.value.hasAtomics()) {
        // the `top` array itself and a sub path of `top` are being modified.
        // the only way to honor all of both modifications is through a $set
        // of entire array.
        top.value._atomics = {};
        top.value._atomics.$set = top.value;
      }
    }
  });

  top = lastPath = null;
  return minimal;
};

function cleanModifiedSubpaths(doc, path) {
  var _modifiedPaths = Object.keys(doc.$__.activePaths.states.modify);
  var _numModifiedPaths = _modifiedPaths.length;
  for (var j = 0; j < _numModifiedPaths; ++j) {
    if (_modifiedPaths[j].indexOf(path + '.') === 0) {
      delete doc.$__.activePaths.states.modify[_modifiedPaths[j]];
    }
  }
}

Document.prototype.isSelected = function isSelected(path) {
  if (this.$__.selected) {
    if (path === '_id') {
      return this.$__.selected._id !== 0;
    }

    var paths = Object.keys(this.$__.selected),
        i = paths.length,
        inclusive = false,
        cur;

    if (i === 1 && paths[0] === '_id') {
      // only _id was selected.
      return this.$__.selected._id === 0;
    }

    while (i--) {
      cur = paths[i];
      if (cur === '_id') {
        continue;
      }
      inclusive = !!this.$__.selected[cur];
      break;
    }

    if (path in this.$__.selected) {
      return inclusive;
    }

    i = paths.length;
    var pathDot = path + '.';

    while (i--) {
      cur = paths[i];
      if (cur === '_id') {
        continue;
      }

      if (cur.indexOf(pathDot) === 0) {
        return inclusive;
      }

      if (pathDot.indexOf(cur + '.') === 0) {
        return inclusive;
      }
    }

    return !inclusive;
  }

  return true;
};

Document.prototype.$toObject = function(options, json) {
  var defaultOptions = {
    transform: true,
    json: json,
    retainKeyOrder: this.schema.options.retainKeyOrder
  };

  // _isNested will only be true if this is not the top level document, we
  // should never depopulate
  if (options && options.depopulate && options._isNested && this.$__.wasPopulated) {
    // populated paths that we set to a document
    return clone(this._id, options);
  }

  // When internally saving this document we always pass options,
  // bypassing the custom schema options.
  if (!(options && utils.getFunctionName(options.constructor) === 'Object') ||
      (options && options._useSchemaOptions)) {
    if (json) {
      options = this.schema.options.toJSON ?
        clone(this.schema.options.toJSON) :
        {};
      options.json = true;
      options._useSchemaOptions = true;
    } else {
      options = this.schema.options.toObject ?
        clone(this.schema.options.toObject) :
        {};
      options.json = false;
      options._useSchemaOptions = true;
    }
  }

  for (var key in defaultOptions) {
    if (options[key] === undefined) {
      options[key] = defaultOptions[key];
    }
  }

  ('minimize' in options) || (options.minimize = this.schema.options.minimize);

  // remember the root transform function
  // to save it from being overwritten by sub-transform functions
  var originalTransform = options.transform;

  options._isNested = true;

  var ret = clone(this._doc, options) || {};

  if (options.getters) {
    applyGetters(this, ret, 'paths', options);
    // applyGetters for paths will add nested empty objects;
    // if minimize is set, we need to remove them.
    if (options.minimize) {
      ret = minimize(ret) || {};
    }
  }

  if (options.virtuals || options.getters && options.virtuals !== false) {
    applyGetters(this, ret, 'virtuals', options);
  }

  if (options.versionKey === false && this.schema.options.versionKey) {
    delete ret[this.schema.options.versionKey];
  }

  var transform = options.transform;

  // In the case where a subdocument has its own transform function, we need to
  // check and see if the parent has a transform (options.transform) and if the
  // child schema has a transform (this.schema.options.toObject) In this case,
  // we need to adjust options.transform to be the child schema's transform and
  // not the parent schema's
  if (transform === true ||
      (this.schema.options.toObject && transform)) {
    var opts = options.json ? this.schema.options.toJSON : this.schema.options.toObject;

    if (opts) {
      transform = (typeof options.transform === 'function' ? options.transform : opts.transform);
    }
  } else {
    options.transform = originalTransform;
  }

  if (typeof transform === 'function') {
    var xformed = transform(this, ret, options);
    if (typeof xformed !== 'undefined') {
      ret = xformed;
    }
  }

  return ret;
};

Document.prototype.toObject = function(options) {
  return this.$toObject(options);
};


module.exports = exports = Document;
