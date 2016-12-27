
var Kareem = require('kareem');
var EventEmitter = require('events').EventEmitter;
var cassandraDriver = require('cassandra-driver');

var UUIDType = require('./schema/uuid');
var utils = require("./utils")

var error = require("./error");
var ValidationError = error.ValidatorError;
var ValidatorError = error.ValidationError;

Schema.Types = require('./schema/index');
Types = Schema.Types;

var VirtualType = require('./virtualtype');

var IS_KAREEM_HOOK = {
  count: true,
  find: true,
  findOne: true,
  findOneAndUpdate: true,
  findOneAndRemove: true,
  insertMany: true,
  update: true
};


function Schema (obj, options) {
  if (!(this instanceof Schema)) {
    return new Schema(obj, insensitive);
  }

  this.nested = {};
  this.paths = {};
  this.subpaths = {};
  this.tree = {};

  this.virtuals = {};
  this.singleNestedPaths = {};

  this.methods = {};
  this.statics = {};

  this.callQueue = [];

  if(options === true) {
    this.options = {
      insensitive: true
    };
  } else if(options) {
    this.options = options;
  } else {
    this.options = {};
  }

  this.insensitive = !!this.options.insensitive;

  this.s = {
    hooks: new Kareem(),
    kareemHooks: IS_KAREEM_HOOK
  };

  if (obj) {
    if(!obj.id) {
      obj.id = {
        type: UUIDType,
        required: true,
        trim: true,
        default: cassandraDriver.types.uuid
      };
    }

    this.add(obj);
  }

  for (var i = 0; i < this._defaultMiddleware.length; ++i) {
    var m = this._defaultMiddleware[i];
    this[m.kind](m.hook, !!m.isAsync, m.fn);
  }

}

Schema.prototype = Object.create(EventEmitter.prototype);
Schema.prototype.constructor = Schema;
Schema.prototype.instanceOfSchema = true;

Schema.prototype.tree;
Schema.prototype.paths;
Schema.prototype.obj;

Schema.prototype.select = function(modelName, conditions, fields, limit) {
  var select = this.selectFields(fields);
  var params = [];

  var query = "SELECT " + select + " FROM " + modelName;
  if(conditions) {
    var where = '';
    for(var p in conditions) {
      if(this.paths[p]) {
        if(where.length > 0) {
          where += " AND ";
        }
        where += this.paths[p].options.name + " = ?";
        params.push(this.paths[p].castForQuery( conditions[p]) );
      } else {
        console.log('select ' + p);
      }
    }
    if(where.length > 0) {
      query += " WHERE " + where;
    }
  }
  if(limit) {
    query += " LIMIT " + limit;
  }
  console.log("[cassandrom] " + query);
  return {
    query: query,
    params: params
  };
};

Schema.prototype._insertValue = function(path, obj, name, fields, params, validationError) {
  var value = obj[name];
  var namePath =  this.paths[path];
  if(namePath) {
    namePath.doValidate(value, function (err) {
      if(err) {
        //gotError = true;
        if (!validationError) {
          validationError = new ValidationError(obj);
        }
        var validatorError = new ValidatorError(p, err, 'user defined', val)
        validationError.errors[p] = validatorError;
      }
    }, obj);
    var options = namePath.options;
    if(fields.length > 0) {
      if(fields.indexOf(path) >= 0 ) {
        params[options.name] = namePath.castForQuery( value );
      }
    } else {
      params[options.name] = namePath.castForQuery( value );
    }
  } else if(value && path in this.nested) {
    for(var subname in this.tree[path]) {
      validationError = this._insertValue(name + '.' + subname, value, subname, fields, params, validationError);
    }
  }
  return validationError;
};

Schema.prototype.update = function(modelName, obj, where, delta, fields) {
  var list =[];
  if(fields && Array.isArray(fields)) {
    list = fields;
  }

  var paramsMap = {};
  var validationError;

  console.log('udpate', where, delta);

  for(var p in this.tree) {
    validationError = this._insertValue(p, obj, p, list, paramsMap, validationError);
  }

  var query =  "UPDATE " + modelName + " (";
  var values = '';
  var params = [];
  for(var name in paramsMap) {
    if(params.length === 0) {
      query += name.replace('\.', '_');
      values += '?';
      params.push(paramsMap[name]);
    } else {
      query += ', ' + name.replace('\.', '_');
      values += ', ?';
      params.push(paramsMap[name]);
    }
  }
  query += ') VALUES (' + values + ')';

  console.log('[cassandrom] ' + query);
  return {
    query: query,
    params: params,
    errors: validationError
  };
// update(this.modelName, this, where, delta[1], options)
};

Schema.prototype.insert = function(modelName, obj, fields) {
  var list =[];
  if(fields && Array.isArray(fields)) {
    list = fields;
  }

  var paramsMap = {};
  var validationError;

  for(var p in this.tree) {
    validationError = this._insertValue(p, obj, p, list, paramsMap, validationError);
  }

  var query =  "INSERT INTO " + modelName + " (";
  var values = '';
  var params = [];
  for(var name in paramsMap) {
    if(params.length === 0) {
      query += name.replace('\.', '_');
      values += '?';
      params.push(paramsMap[name]);
    } else {
      query += ', ' + name.replace('\.', '_');
      values += ', ?';
      params.push(paramsMap[name]);
    }
  }
  query += ') VALUES (' + values + ')';

  console.log('[cassandrom] ' + query);

  return {
    query: query,
    params: params,
    errors: validationError
  };
};


Schema.prototype._select = function(name) {
  var schema = this.paths[name];
  var select = null;
  if(schema) {
    if(schema.instance !== "ObjectID") {
      select = this.paths[name].options.name;
    }
  } else if(name in this.nested) {
    select = '';
    var field;
    for(var subname in this.tree[name]) {
      field = this._select(name + '.' + subname);
      if(field !== null) {
        if(select.length > 0) {
          select += ', ';
        }
        select += field.replace('\.', '_');
      }
    }
  }
  return select;
};

var CQL_FUNCTIONS = {
  'count': true
};

function _isFunction(field) {
  var ind = field.indexOf('(');
  if(ind >= 0) {
    var name = field.substring(0, ind);
    return CQL_FUNCTIONS[name];
  }
  return false;
};

Schema.prototype.selectFields = function(fields) {
  var list =[];
  if(fields && Array.isArray(fields)) {
    list = fields;
  }
  var used = {};
  var select = '', fieldSelect, index;
  for(var p in this.tree) {
    fieldSelect = null;
    if(list.length > 0) {
      index = list.indexOf(p);
      // console.log('   ', list, index, p);
      if(index >= 0) {
        fieldSelect = this._select(p);
        used[p] = true;
        // console.log(fieldSelect, ' = ', p);
      }
    } else {
      fieldSelect = this._select(p);
    }
    if(fieldSelect !== null) {
      if(select.length > 0) {
        select += ", ";
      }
      select += fieldSelect;
    }
  }

  if(list.length > 0) {
    for(var i = 0; i < list.length; i++) {
      if(!used[list[i]] && _isFunction(list[i])) {
        if(select.length > 0) {
          select += ", ";
        }
        select += list[i];
      }
    }
  }

  if(select.length === 0) {
    select = "*";
  }
  return select;
};

Schema.prototype.add = function add (obj, prefix) {
  prefix = prefix || '';
  var keys = Object.keys(obj);

  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];

    if (null == obj[key]) {
      throw new TypeError('Invalid value for schema path `'+ prefix + key +'`');
    }

    var desc = obj[key];

    if (utils.isObject(desc)
      && (!desc.constructor || 'Object' == desc.constructor.name)
      && (!desc.type || desc.type.type)) {
      if (Object.keys(desc).length) {
        // nested object { last: { name: String }}
        this.nested[prefix + key] = true;
        this.add(desc, prefix + key + '.');
      } else {
        if (prefix) {
          this.nested[prefix.substr(0, prefix.length - 1)] = true;
        }
        this.path(prefix + key, desc); // mixed type
      }
    } else {
      if (prefix) {
        this.nested[prefix.substr(0, prefix.length - 1)] = true;
      }
      this.path(prefix + key, desc);
    }
  }
};

Schema.reserved = Object.create(null);
var reserved = Schema.reserved;
reserved['prototype'] =
reserved.on =
reserved.db =
reserved.set =
reserved.get =
reserved.init =
reserved.isNew =
reserved.errors =
reserved.schema =
reserved.options =
reserved.modelName =
reserved.collection =
reserved.toObject =
reserved.emit =    // EventEmitter
reserved._events = // EventEmitter
reserved._pres = reserved._posts = 1 // hooks.js

Schema.prototype.path = function (path, obj) {
  // get path
  if (obj == undefined) {
    if (this.paths[path]) {
      return this.paths[path];
    }

    if (this.subpaths[path]) {
      return this.subpaths[path];
    }

    if (this.singleNestedPaths[path]) {
      return this.singleNestedPaths[path];
    }

    // subpaths?
    return /\.\d+\.?.*$/.test(path)
      ? getPositionalPath(this, path)
      : undefined;
  }

  // some path names conflict with document methods
  if (reserved[path]) {
    throw new Error("`" + path + "` may not be used as a schema pathname");
  }

  // update the tree
  var subpaths = path.split(/\./)
    , last = subpaths.pop()
    , branch = this.tree;

  subpaths.forEach(function(sub, i) {
    if (!branch[sub]) {
      branch[sub] = {};
    }
    if ('object' !== typeof branch[sub]) {
      var msg = 'Cannot set nested path `' + path + '`. '
              + 'Parent path `'
              + subpaths.slice(0, i).concat([sub]).join('.')
              + '` already set to type ' + branch[sub].name
              + '.';
      throw new Error(msg);
    }
    branch = branch[sub];
  });

  branch[last] = utils.clone(obj);

  this.paths[path] = Schema.interpretAsType(this.insensitive, path, obj);

  // if (this.paths[path].$isSingleNested) {
  //   for (var key in this.paths[path].schema.paths) {
  //     this.singleNestedPaths[path + '.' + key] =
  //         this.paths[path].schema.paths[key];
  //   }
  //   for (key in this.paths[path].schema.singleNestedPaths) {
  //     this.singleNestedPaths[path + '.' + key] =
  //         this.paths[path].schema.singleNestedPaths[key];
  //   }

  //   this.childSchemas.push(this.paths[path].schema);
  // } else if (this.paths[path].$isMongooseDocumentArray) {
  //   this.childSchemas.push(this.paths[path].schema);
  // }

  return this;
};

Schema.interpretAsType = function (insensitive, path, obj) {
  if (obj.constructor && obj.constructor.name != 'Object') {
    obj = { type: obj };
  }

  // Get the type making sure to allow keys named "type"
  // and default to mixed if not specified.
  // { type: { type: String, default: 'freshcut' } }
  var type = obj.type && !obj.type.type
    ? obj.type
    : {};

  // if ('Object' == type.constructor.name || 'mixed' == type) {
  //   return new Types.Mixed(path, obj);
  // }

  if(!obj.name) {
    if(insensitive) {
      obj.name = path.toLowerCase();
    } else {
      obj.name = path;
    }
  }

  if (Array.isArray(type) || Array == type) {
    // if it was specified through { type } look for `cast`
    var cast = Array == type
      ? obj.cast
      : type[0];
    return new Types.Collection(path, cast, obj);
  }

  var name = 'string' == typeof type
    ? type
    : type.name;

  if (name) {
    name = name.charAt(0).toUpperCase() + name.substring(1);
  }

  if (undefined == Types[name]) {
    throw new TypeError('Undefined type at `' + path +
        '`\n  Did you try nesting Schemas? ' +
        'You can only nest using refs or arrays.');
  }

  return new Types[name](path, obj);
};


Schema.prototype.build = function(name) {
  // obj, fields, skipId
  var doc = {}
    , self = this
    , exclude
    , keys
    , key
    , ki

  // determine if this doc is a result of a query with
  // excluded fields
  if (fields && 'Object' === fields.constructor.name) {
    keys = Object.keys(fields);
    ki = keys.length;

    while (ki--) {
      if ('_id' !== keys[ki]) {
        exclude = 0 === fields[keys[ki]];
        break;
      }
    }
  }

  var paths = Object.keys(this.schema.paths)
    , plen = paths.length
    , ii = 0

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
        , def

      // support excluding intermediary levels
      if (exclude) {
        curPath += piece;
        if (curPath in fields) break;
        curPath += '.';
      }

      if (i === last) {
        if (fields) {
          if (exclude) {
            // apply defaults to all non-excluded fields
            if (p in fields) continue;

            def = type.getDefault(self, true);
            if ('undefined' !== typeof def) {
              doc_[piece] = def;
              self.$__.activePaths.default(p);
            }

          } else if (p in fields) {
            // selected field
            def = type.getDefault(self, true);
            if ('undefined' !== typeof def) {
              doc_[piece] = def;
              self.$__.activePaths.default(p);
            }
          }
        } else {
          def = type.getDefault(self, true);
          if ('undefined' !== typeof def) {
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

Schema.prototype.static = function(name, fn) {
  if ('string' !== typeof name) {
    for (var i in name) {
      this.statics[i] = name[i];
    }
  } else {
    this.statics[name] = fn;
  }
  return this;
};

Schema.prototype.method = function (name, fn) {
  if ('string' !== typeof name) {
    for (var i in name) {
      this.methods[i] = name[i];
    }
  } else {
    this.methods[name] = fn;
  }
  return this;
};

Schema.prototype.virtual = function(name, options) {
  if (options && options.ref) {
    this.pre('init', function(next, obj) {
      if (name in obj) {
        if (!this.$$populatedVirtuals) {
          this.$$populatedVirtuals = {};
        }

        if (options.justOne) {
          this.$$populatedVirtuals[name] = Array.isArray(obj[name]) ?
            obj[name][0] :
            obj[name];
        } else {
          this.$$populatedVirtuals[name] = Array.isArray(obj[name]) ?
            obj[name] :
            obj[name] == null ? [] : [obj[name]];
        }

        delete obj[name];
      }
      if (this.ownerDocument) {
        next();
        return obj;
      } else {
        next();
      }
    });

    var virtual = this.virtual(name);
    virtual.options = options;
    return virtual.
      get(function() {
        if (!this.$$populatedVirtuals) {
          this.$$populatedVirtuals = {};
        }
        if (name in this.$$populatedVirtuals) {
          return this.$$populatedVirtuals[name];
        }
        return null;
      }).
      set(function(v) {
        if (!this.$$populatedVirtuals) {
          this.$$populatedVirtuals = {};
        }
        this.$$populatedVirtuals[name] = v;
      });
  }

  var virtuals = this.virtuals;
  var parts = name.split('.');

  if (this.pathType(name) === 'real') {
    throw new Error('Virtual path "' + name + '"' +
      ' conflicts with a real path in the schema');
  }

  virtuals[name] = parts.reduce(function(mem, part, i) {
    mem[part] || (mem[part] = (i === parts.length - 1)
        ? new VirtualType(options, name)
        : {});
    return mem[part];
  }, this.tree);

  return virtuals[name];
};

Schema.prototype.pathType = function(path) {
  if (path in this.paths) {
    return 'real';
  }
  if (path in this.virtuals) {
    return 'virtual';
  }
  if (path in this.nested) {
    return 'nested';
  }
  if (path in this.subpaths) {
    return 'real';
  }
  if (path in this.singleNestedPaths) {
    return 'real';
  }

  return 'adhocOrUndefined';
};

Schema.prototype.post = function(method, fn) {
  if (IS_KAREEM_HOOK[method]) {
    this.s.hooks.post.apply(this.s.hooks, arguments);
    return this;
  }
  // assuming that all callbacks with arity < 2 are synchronous post hooks
  if (fn.length < 2) {
    return this.queue('on', [arguments[0], function(doc) {
      return fn.call(doc, doc);
    }]);
  }

  if (fn.length === 3) {
    this.s.hooks.post(method + ':error', fn);
    return this;
  }

  return this.queue('post', [arguments[0], function(next) {
    // wrap original function so that the callback goes last,
    // for compatibility with old code that is using synchronous post hooks
    var _this = this;
    var args = Array.prototype.slice.call(arguments, 1);
    fn.call(this, this, function(err) {
      return next.apply(_this, [err].concat(args));
    });
  }]);
};


Schema.prototype.pre = function() {
  var name = arguments[0];
  if (IS_KAREEM_HOOK[name]) {
    this.s.hooks.pre.apply(this.s.hooks, arguments);
    return this;
  }
  return this.queue('pre', arguments);
};

Schema.prototype.post = function(method, fn) {
  // assuming that all callbacks with arity < 2 are synchronous post hooks
  if (fn.length < 2) {
    return this.queue('on', [arguments[0], function(doc) {
      return fn.call(doc, doc);
    }]);
  }

  // if (fn.length === 3) {
  //   this.s.hooks.post(method + ':error', fn);
  //   return this;
  // }

  return this.queue('post', [arguments[0], function(next) {
    // wrap original function so that the callback goes last,
    // for compatibility with old code that is using synchronous post hooks
    var _this = this;
    var args = Array.prototype.slice.call(arguments, 1);
    fn.call(this, this, function(err) {
      return next.apply(_this, [err].concat(args));
    });
  }]);
};

Schema.prototype.queue = function(name, args) {
  this.callQueue.push([name, args]);
  return this;
};

Schema.prototype.get = function(key) {
  return this.options[key];
};


Object.defineProperty(Schema.prototype, '_defaultMiddleware', {
  configurable: false,
  enumerable: false,
  writable: false,
  value: [
    {
      kind: 'pre',
      hook: 'save',
      fn: function(next, options) {
        var _this = this;
        // Nested docs have their own presave
        if (this.ownerDocument) {
          return next();
        }

        var hasValidateBeforeSaveOption = options &&
            (typeof options === 'object') &&
            ('validateBeforeSave' in options);

        var shouldValidate;
        if (hasValidateBeforeSaveOption) {
          shouldValidate = !!options.validateBeforeSave;
        } else {
          shouldValidate = this.schema.options.validateBeforeSave;
        }

        // Validate
        if (shouldValidate) {
          // HACK: use $__original_validate to avoid promises so bluebird doesn't
          // complain
          if (this.$__original_validate) {
            this.$__original_validate({__noPromise: true}, function(error) {
              return _this.schema.s.hooks.execPost('save:error', _this, [_this], { error: error }, function(error) {
                next(error);
              });
            });
          } else {
            this.validate({__noPromise: true}, function(error) {
              return _this.schema.s.hooks.execPost('save:error', _this, [ _this], { error: error }, function(error) {
                next(error);
              });
            });
          }
        } else {
          next();
        }
      }
    },
    {
      kind: 'pre',
      hook: 'save',
      isAsync: true,
      fn: function(next, done) {
        var _this = this;
        var subdocs = this.$__getAllSubdocs();

        if (!subdocs.length || this.$__preSavingFromParent) {
          done();
          next();
          return;
        }

        each(subdocs, function(subdoc, cb) {
          subdoc.$__preSavingFromParent = true;
          subdoc.save(function(err) {
            cb(err);
          });
        }, function(error) {
          for (var i = 0; i < subdocs.length; ++i) {
            delete subdocs[i].$__preSavingFromParent;
          }
          if (error) {
            return _this.schema.s.hooks.execPost('save:error', _this, [_this], { error: error }, function(error) {
              done(error);
            });
          }
          next();
          done();
        });
      }
    },
    {
      kind: 'pre',
      hook: 'validate',
      isAsync: true,
      fn: function(next, done) {
        // Hack to ensure that we always wrap validate() in a promise
        next();
        done();
      }
    },
    {
      kind: 'pre',
      hook: 'remove',
      isAsync: true,
      fn: function(next, done) {
        if (this.ownerDocument) {
          done();
          next();
          return;
        }

        var subdocs = this.$__getAllSubdocs();

        if (!subdocs.length || this.$__preSavingFromParent) {
          done();
          next();
          return;
        }

        each(subdocs, function(subdoc, cb) {
          subdoc.remove({ noop: true }, function(err) {
            cb(err);
          });
        }, function(error) {
          if (error) {
            done(error);
            return;
          }
          next();
          done();
        });
      }
    }
  ]
});

Schema.prototype.virtualpath = function(name) {
  return this.virtuals[name];
};

/**
 * Returns an Array of path strings that are required by this schema.
 *
 * @api public
 * @param {Boolean} invalidate refresh the cache
 * @return {Array}
 */

Schema.prototype.requiredPaths = function requiredPaths(invalidate) {
  if (this._requiredpaths && !invalidate) {
    return this._requiredpaths;
  }

  var paths = Object.keys(this.paths),
      i = paths.length,
      ret = [];

  while (i--) {
    var path = paths[i];
    if (this.paths[path].isRequired) {
      ret.push(path);
    }
  }
  this._requiredpaths = ret;
  return this._requiredpaths;
};


module.exports = exports = Schema;
