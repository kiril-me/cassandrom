var utils = require('./utils');
var error = require('./error');

//var when = require('when');

function Model(doc, fields) {

  console.log('model: ' + JSON.stringify(doc));
  //this.connections = [];
  //this.plugins = [];
  //this.models = {};
  //this.modelSchemas = {};

  this.options = {

  };

  this.isNew = true;
  this.errors = undefined;

  var obj = this.$__normalize(doc);

  this._doc = this.$__buildDoc(obj, fields);

  if (doc) {
    this.set(obj, undefined, true);
  }
}

Model.prototype.schema;

Model.prototype.modelName;

Model.prototype.db;

Model.init = function init () {
  // if (this.schema.options.autoIndex) {
  //   this.ensureIndexes();
  // }

  // this.schema.emit('init', this);
};

Model.prototype.$__normalize = function(obj) {
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

Model.prototype.$__buildDoc = function (obj, fields) {
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
      // if (exclude) {
      //   curPath += piece;
      //   if (curPath in fields) break;
      //   curPath += '.';
      // }

      if (i === last) {
        if (fields) {
          // if (exclude) {
          //   // apply defaults to all non-excluded fields
          //   if (p in fields) continue;

          //   def = type.getDefault(self, true);
          //   if ('undefined' !== typeof def) {
          //     doc_[piece] = def;
          //     self.$__.activePaths.default(p);
          //   }

          // } else
          if (p in fields) {
            // selected field
            def = type.getDefault(self, true);
            if ('undefined' !== typeof def) {
              doc_[piece] = def;
              //self.$__.activePaths.default(p);
            }
          }
        } else {
          def = type.getDefault(self, true);
          if ('undefined' !== typeof def) {
            doc_[piece] = def;
            // self.$__.activePaths.default(p);
          }
        }
      } else {
        doc_ = doc_[piece] || (doc_[piece] = {});
      }
    }
  };

  return doc;
};

// Model.prototype.getValue = function (path) {
//   console.log(" ##### " + JSON.stringify( this._doc ) + ' path ' + path);
//   return this._doc[path];
//   // return utils.getValue(path, this._doc);
// }

Model.prototype.save = function save (fields, fn) {
  if ('function' == typeof fields) {
    fn = fields;
    fields = null;
  }

  if (this.isNew) {
    // for(var p in this) {
    //   console.log(p);
    // }
    var insert = this.schema.insert(this.modelName, this, fields), self = this;
    //console.log(insert.query);
    if(insert.error) {
      fn(insert.error);
    } else {
      this.db.execute(insert.query, insert.params, {prepare: true}, function(error, status) {
        if(error) {
          fn(error);
        } else {
          // console.log(JSON.stringify(status));

          // TODO check status.opcode??

          self.isNew = false;
          fn(null, self);
        }
      });
    }
  } else {
    // TODO update;
  }
};

Model.prototype.set = function (path, val, type, options) {
  if (type && 'Object' == type.constructor.name) {
    options = type;
    type = undefined;
  }

  var merge = options && options.merge
   // , adhoc = type && true !== type
    , constructing = true === type
    , adhocs;

  // if (adhoc) {
  //   adhocs = this.$__.adhocPaths || (this.$__.adhocPaths = {});
  //   adhocs[path] = Schema.interpretAsType(path, type);
  // }

  if ('string' !== typeof path) {
    // new Document({ key: val })

    if (null === path || undefined === path) {
      var _ = path;
      path = val;
      val = _;

    } else {
      var prefix = val
        ? val + '.'
        : '';

      if (path instanceof Model) {
        path = path._doc;
      }

      var keys = Object.keys(path)
        , i = keys.length
        // , pathtype
        , key


      while (i--) {
        key = keys[i];
        //pathtype = this.schema.pathType(prefix + key);

//console.log('@@ ' + key + ": " + path[key] + "    " + utils.isObject(path[key]));


        if (null != path[key]
            // need to know if plain object - no Buffer, ObjectId, ref, etc
            && utils.isObject(path[key])
            && (!path[key].constructor || 'Object' == path[key].constructor.name)
            // && 'virtual' != pathtype
            // && !(this.$__path(prefix + key) instanceof MixedSchema)
            && !(this.schema.paths[key] && this.schema.paths[key].options.ref)
          ) {
          this.set(path[key], prefix + key, constructing);
        } else if (undefined !== path[key]) {
          this.set(prefix + key, path[key], constructing);
        }
        // else {
        //   console.log(prefix + ", " + key + ", " + path[key] + ", " + constructing);
        //   throw new Error("Field `" + key + "` is not in schema.");
        // }


        // else if (strict) {
        //   if ('real' === pathtype || 'virtual' === pathtype) {
        //     this.set(prefix + key, path[key], constructing);
        //   } else if ('throw' == strict) {
        //     throw new Error("Field `" + key + "` is not in schema.");
        //   }
        // } else if (undefined !== path[key]) {
        //   this.set(prefix + key, path[key], constructing);
        // }
      }

      return this;
    }
  }

  // ensure _strict is honored for obj props
  // docschema = new Schema({ path: { nest: 'string' }})
  // doc.set('path', obj);
  // var pathType = this.schema.pathType(path);
  // if ('nested' == pathType && val && utils.isObject(val) &&
  //     (!val.constructor || 'Object' == val.constructor.name)) {
  //   if (!merge) this.setValue(path, null);
  //   this.set(val, path, constructing);
  //   return this;
  // }

  // console.log('set schema ' + this.schema );

  var schema;
  var parts = path.split('.');

  // if ('adhocOrUndefined' == pathType && strict) {

  //   // check for roots that are Mixed types
  //   var mixed;

  //   for (var i = 0; i < parts.length; ++i) {
  //     var subpath = parts.slice(0, i+1).join('.');
  //     schema = this.schema.path(subpath);
  //     if (schema instanceof MixedSchema) {
  //       // allow changes to sub paths of mixed types
  //       mixed = true;
  //       break;
  //     }
  //   }

  //   if (!mixed) {
  //     if ('throw' == strict) {
  //       throw new Error("Field `" + path + "` is not in schema.");
  //     }
  //     return this;
  //   }

  // } else if ('virtual' == pathType) {
  //   schema = this.schema.virtualpath(path);
  //   schema.applySetters(val, this);
  //   return this;
  // } else {
    schema = this.schema.path(path); //this.$__path(path);
  // }



  var pathToMark;

  // console.log('set schema ' + schema);

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

  var self = this;
  var shouldSet = this.$__try(function(){
    val = schema.applySetters(val, self, false, priorVal);
  });

  if (shouldSet) {
    this.$__set(pathToMark, path, constructing, parts, schema, val, priorVal);
  }

  return this;
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
  console.log("Error: " + error);
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

Model.create = function create (doc, fields, fn) {
  if ('function' == typeof fields) {
    fn = fields;
    fields = null;
  }

  //var args;
  // promise = new Promise
  var self = this;
  if (Array.isArray(doc)) {
    //args = doc;
    console.log("Not implemented");
    // if ('function' == typeof fn) {
    //   promise.onResolve(fn);
    // }
  } else {
    //console.log('creat model name ' + self.modelName)
    var model = new self(doc);

   // console.log('def ' + model.userId);

    model.save(fields, function (err, result) {
      fn(err, result);
    });

  }
  // } else {
  //   var last  = arguments[arguments.length - 1];

  //   if ('function' == typeof last) {
  //     // promise.onResolve(last);
  //     args = utils.args(arguments, 0, arguments.length - 1);
  //   } else {
  //     args = utils.args(arguments);
  //   }
  // }

  // var count = args.length;
  //
  // if (0 === count) {
  //   promise.complete();
  //   return promise;
  // }


  // var docs = [];

  // args.forEach(function (arg, i) {
  //   var doc = new self(arg);
  //   docs[i] = doc;
  //   doc.save(function (err) {
  //     if (err) {
  //       return promise.error(err);
  //     }
  //     //--count || promise.complete.apply(promise, docs);
  //   });
  // });

  // return promise;
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

Model.find = function find (conditions, fields, limit, callback) {
  // console.log('[cassandrom] find');
  if ('function' == typeof limit) {
    callback = limit;
    limit = null;
  } else if ('function' == typeof fields) {
    callback = fields;
    fields = null;
    limit = null;
  } else if ('function' == typeof conditions) {
    callback = conditions;
    conditions = {};
    fields = null;
    limit = null;
  }

  var select = this.schema.select(this.modelName, conditions, fields, limit);
  this.db.execute(select.query, select.params, {prepare: true}, this.$__result(callback, limit === 1));
}

Model.findOne = function findOne (conditions, fields, callback) {
  // console.log('[cassandrom] findOne');
  if ('function' == typeof fields) {
    callback = fields;
    fields = null;
  } else if ('function' == typeof conditions) {
    callback = conditions;
    conditions = {};
    fields = null;
  }
  this.find(conditions, fields, 1, callback);
};

Model.$__result = function(callback, one) {
  var self = this;
  return function(error, result) {
    if(error) {
      console.log('[cassandrom] Query error: ' + error);
      callback(error, null);
    } else {
      var data = self.$__parseData(self, result, one);
      callback(null, data);
    }
  };
};

Model.$__parseData = function(model, data, one) {
  var list = [];
  if(data.rows && data.rows.length > 0) {
    var i = 0, obj, p, size = one ? 1 : data.rows.length;
    for(; i < size; i++) {
      obj = new model( data.rows[i] );
      list.push( obj );
    }
  }
  if(list.length > 0) {
    if(one) {
      return list[0];
    } else {
      return list;
    }
  }
  return null;
};

Model.prototype.$__setSchema = function (schema) {
  compileObject(schema.tree, this);
  this.schema = schema;
}

Model.prototype.$__setModelName = function (modelName) {
  this.modelName = modelName;
}

Model.compile = function compile (name, schema, collectionName, connection, base) {
  // generate new class
  function model (doc, fields) {
    if (!(this instanceof model)) {
      return new model(doc, fields);
    }
    Model.call(this, doc, fields);
  };

  model.base = base; // Model

  model.__proto__ = Model;
  model.prototype.__proto__ = Model.prototype;
  model.db = model.prototype.db = connection;

  model.prototype.$__setSchema(schema);
  model.schema = model.prototype.schema;

  model.prototype.$__setModelName(name);
  model.modelName = model.prototype.modelName;

  // apply methods
  for (var i in schema.methods) {
    model.prototype[i] = schema.methods[i];
  }

  // apply statics
  for (var i in schema.statics) {
    model[i] = schema.statics[i];
  }
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
          if (v instanceof Document) v = v.toObject();
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

Model.prototype.getValue = function (path) {
  console.log(" ##### " + this._doc[path] + ' path ' + path);
  return this._doc[path];
  //return utils.getValue(path, this._doc);
}

Model.prototype.setValue = function (path, val) {
  utils.setValue(path, val, this._doc);
  return this;
}

Model.prototype.get = function (path, type) {
  //console.log('get ' + path + '  ' + type);
  // var adhocs;
  // if (type) {
  //   adhocs = this.$__.adhocPaths || (this.$__.adhocPaths = {});
  //   adhocs[path] = Schema.interpretAsType(path, type);
  // }
  // schema.options.name.split('.') //

  var schema = this.schema.path(path)
    , pieces = path.split('.')
    , obj = this._doc;

  for (var i = 0, l = pieces.length; i < l; i++) {
    obj = undefined === obj || null === obj
      ? undefined
      : obj[pieces[i]];
  }
// console.log('get ' + path + ' -> ' + obj + '  ' + JSON.stringify(schema));
  if (schema) {
    obj = schema.applyGetters(obj, this);
  }

  return obj;
};

Model.prototype.toObject = function (options) {
  if (options && options.depopulate /* && this.$__.wasPopulated */ ) {
    // populated paths that we set to a document
    return utils.clone(this._id, options);
  }

  // When internally saving this document we always pass options,
  // bypassing the custom schema options.
  var optionsParameter = options;
  if (!(options && 'Object' == options.constructor.name) ||
      (options && options._useSchemaOptions)) {
    options = this.schema.options.toObject
      ? utils.clone(this.schema.options.toObject)
      : {};
  }

  ;('minimize' in options) || (options.minimize = this.schema.options.minimize);
  if (!optionsParameter) {
    options._useSchemaOptions = true;
  }

  var ret = utils.clone(this._doc, options);

  if (options.virtuals || options.getters && false !== options.virtuals) {
    applyGetters(this, ret, 'virtuals', options);
  }

  if (options.getters) {
    applyGetters(this, ret, 'paths', options);
    // applyGetters for paths will add nested empty objects;
    // if minimize is set, we need to remove them.
    if (options.minimize) {
      ret = minimize(ret) || {};
    }
  }

  // In the case where a subdocument has its own transform function, we need to
  // check and see if the parent has a transform (options.transform) and if the
  // child schema has a transform (this.schema.options.toObject) In this case,
  // we need to adjust options.transform to be the child schema's transform and
  // not the parent schema's
  if (true === options.transform ||
      (this.schema.options.toObject && options.transform)) {
    var opts = options.json
      ? this.schema.options.toJSON
      : this.schema.options.toObject;
    if (opts) {
      options.transform = opts.transform;
    }
  }

  if ('function' == typeof options.transform) {
    var xformed = options.transform(this, ret, options);
    if ('undefined' != typeof xformed) ret = xformed;
  }

  return ret;
};

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


module.exports = exports = Model;