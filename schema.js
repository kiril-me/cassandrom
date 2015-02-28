var utils = require("./utils")

Schema.Types = require('./schema/index');
Types = Schema.Types;

function Schema (obj, insensitive) {
  if (!(this instanceof Schema)) {
    return new Schema(obj, options);
  }

  this.nested = {};
  this.paths = {};
  this.subpaths = {};
  this.tree = {};

  this.methods = {};
  this.statics = {};

  this.options = {};

  this.insensitive = insensitive;

  if (obj) {

    this.add(obj);
  }
}

Schema.prototype.tree;
Schema.prototype.paths;

Schema.prototype.select = function(modelName, conditions, fields, limit) {
  var select = this.selectFields(fields);
  var where = "";
  var params = [];
  for(var p in conditions) {
    if(where.length > 0) {
      where += " AND ";
    }
    where += this.paths[p].options.name + " = ?"

    //var val = obj.getValue(p);
    params.push(this.paths[p].castForQuery( conditions[p]) );
  }

  var query = "SELECT " + select + " FROM " + modelName;
  if(where.length > 0) {
    query += " WHERE " + where;
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

Schema.prototype.insert = function(modelName, obj, fields) {
  var list =[];
  if(fields && Array.isArray(fields)) {
    list = fields;
  }

  var names = "";
  var values = "";
  var params = [];
  var validationError;

  for(var p in this.tree) {
    var path = this.paths[p];
    var val = obj[p];

    //if (schematype.options.required) {
    //var gotError = false;

    path.doValidate(val, function (err) {
      if(err) {
        //gotError = true;
        if (!validationError) {
          validationError = new ValidationError(obj);
        }
        var validatorError = new ValidatorError(p, err, 'user defined', val)
        validationError.errors[p] = validatorError;
      }
    }, obj);
    //}

    if(val) {
      var options = path.options;
      if(list.length > 0) {
        if(list.indexOf(p) >=0 ) {
          if(values.length > 0) {
            names += ", ";
            values += ", ";
          }
          names += options.name;
          values += "?";
          params.push(path.castForQuery( val ));
        }
      } else {
        if(values.length > 0) {
          names += ", ";
          values += ", ";
        }
        names += options.name;
        values += "?";

        params.push(path.castForQuery( val ));
      }
    }
  }

  var query =  "INSERT INTO " + modelName + " (" + names + ") VALUES (" + values + ")";

  return {
    query: query,
    params: params,
    errors: validationError
  };
};

Schema.prototype.selectFields = function(fields) {
  var list =[];
  if(fields && Array.isArray(fields)) {
    list = fields;
  }

  var select = "";
  for(var p in this.tree) {
    var schema = this.paths[p];

    if(schema.instance !== "ObjectID") {
      if(list.length > 0) {
        if(list.indexOf(p) >=0 ) {
          if(select.length > 0) {
            select += ", ";
          }
          select += this.paths[p].options.name;
        }
      } else {
        if(select.length > 0) {
          select += ", ";
        }
        select += this.paths[p].options.name;
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
        this.path(prefix + key, desc); // mixed type
      }
    } else {
      this.path(prefix + key, desc);
    }
  }
};

Schema.reserved = Object.create(null);
var reserved = Schema.reserved;
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

    // if (this.subpaths[path]) return this.subpaths[path];

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
    if (!branch[sub]) branch[sub] = {};
    if ('object' != typeof branch[sub]) {
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
  return this;
};

Schema.interpretAsType = function (insensitive, path, obj) {
  if (obj.constructor && obj.constructor.name != 'Object')
    obj = { type: obj };

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

  if (Array.isArray(type) || Array == type || 'array' == type) {
    // if it was specified through { type } look for `cast`
    var cast = (Array == type || 'array' == type)
      ? obj.cast
      : type[0];

    return new Types.Array(path, cast, obj);
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
  if ('string' != typeof name)
    for (var i in name)
      this.statics[i] = name[i];
  else
    this.statics[name] = fn;
  return this;
};

Schema.prototype.method = function (name, fn) {
  if ('string' != typeof name)
    for (var i in name)
      this.methods[i] = name[i];
  else
    this.methods[name] = fn;
  return this;
};

module.exports = exports = Schema;
