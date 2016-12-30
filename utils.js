var sliced = require('sliced');
var mpath = require('mpath');
var Document;

var toString = Object.prototype.toString;

/*!
 * Return the value of `obj` at the given `path`.
 *
 * @param {String} path
 * @param {Object} obj
 */

exports.getValue = function(path, obj, map) {
  return mpath.get(path, obj, '_doc', map);
};

/*!
 * Sets the value of `obj` at the given `path`.
 *
 * @param {String} path
 * @param {Anything} val
 * @param {Object} obj
 */

exports.setValue = function(path, val, obj, map) {
  mpath.set(path, val, obj, '_doc', map);
};

exports.isObject = function (arg) {
  return '[object Object]' == toString.call(arg);
}

exports.args = sliced;

exports.clone = function clone (obj, options) {
  if (obj === undefined || obj === null)
    return obj;

  if (Array.isArray(obj))
    return cloneArray(obj, options);

  if (obj.constructor) {
    switch (obj.constructor.name) {
      case 'Object':
        return cloneObject(obj, options);
      case 'Date':
        return new obj.constructor(+obj);
      // case 'RegExp':
      //   return cloneRegExp(obj);
      default:
        // ignore
        break;
    }
  }

  // if (obj instanceof ObjectId)
  //   return new ObjectId(obj.id);

  if (!obj.constructor && exports.isObject(obj)) {
    // object created with Object.create(null)
    return cloneObject(obj, options);
  }

  if (obj.valueOf)
    return obj.valueOf();
};
var clone = exports.clone;

function cloneArray (arr, options) {
  var ret = [];
  for (var i = 0, l = arr.length; i < l; i++)
    ret.push(clone(arr[i], options));
  return ret;
};

function cloneObject (obj, options) {
  var retainKeyOrder = options && options.retainKeyOrder
    , minimize = options && options.minimize
    , ret = {}
    , hasKeys
    , keys
    , val
    , k
    , i

  if (retainKeyOrder) {
    for (k in obj) {
      val = clone(obj[k], options);

      if (!minimize || ('undefined' !== typeof val)) {
        hasKeys || (hasKeys = true);
        ret[k] = val;
      }
    }
  } else {
    // faster

    keys = Object.keys(obj);
    i = keys.length;

    while (i--) {
      k = keys[i];
      val = clone(obj[k], options);

      if (!minimize || ('undefined' !== typeof val)) {
        if (!hasKeys) hasKeys = true;
        ret[k] = val;
      }
    }
  }

  return minimize
    ? hasKeys && ret
    : ret;
};

exports.each = function(arr, fn) {
  for (var i = 0; i < arr.length; ++i) {
    fn(arr[i]);
  }
};

exports.object = {};

exports.object.vals = function vals(o) {
  var keys = Object.keys(o),
      i = keys.length,
      ret = [];

  while (i--) {
    ret.push(o[keys[i]]);
  }

  return ret;
};

var hop = Object.prototype.hasOwnProperty;
exports.object.hasOwnProperty = function(obj, prop) {
  return hop.call(obj, prop);
};

exports.getFunctionName = function(fn) {
  if (fn.name) {
    return fn.name;
  }
  return (fn.toString().trim().match(/^function\s*([^\s(]+)/) || [])[1];
};

exports.isNullOrUndefined = function(val) {
  return val === null || val === undefined;
};

exports.toObject = function toObject(obj) {
  console.log('toObject: ' + obj);

  Document || (Document = require('./document'));
  var ret;

  if (exports.isNullOrUndefined(obj)) {
    return obj;
  }

  if (obj instanceof Document) {
    return obj.toObject();
  }

  if (Array.isArray(obj)) {
    ret = [];

    for (var i = 0, len = obj.length; i < len; ++i) {
      ret.push(toObject(obj[i]));
    }

    return ret;
  }

  if ((obj.constructor && exports.getFunctionName(obj.constructor) === 'Object') ||
      (!obj.constructor && exports.isObject(obj))) {
    ret = {};

    for (var k in obj) {
      ret[k] = toObject(obj[k]);
    }

    return ret;
  }

  return obj;
};

exports.merge = function merge(to, from, options) {
  options = options || {};
  var keys = Object.keys(from);
  var i = 0;
  var len = keys.length;
  var key;

  if (options.retainKeyOrder) {
    while (i < len) {
      key = keys[i++];
      if (typeof to[key] === 'undefined') {
        to[key] = from[key];
      } else if (exports.isObject(from[key])) {
        merge(to[key], from[key]);
      } else if (options.overwrite) {
        to[key] = from[key];
      }
    }
  } else {
    while (len--) {
      key = keys[len];
      if (typeof to[key] === 'undefined') {
        to[key] = from[key];
      } else if (exports.isObject(from[key])) {
        merge(to[key], from[key]);
      } else if (options.overwrite) {
        to[key] = from[key];
      }
    }
  }
};

exports.mergeClone = function(to, fromObj) {
  var keys = Object.keys(fromObj);
  var len = keys.length;
  var i = 0;
  var key;

  while (i < len) {
    key = keys[i++];
    if (typeof to[key] === 'undefined') {
      // make sure to retain key order here because of a bug handling the $each
      // operator in mongodb 2.4.4
      to[key] = exports.clone(fromObj[key], {retainKeyOrder: 1});
    } else {
      if (exports.isObject(fromObj[key])) {
        var obj = fromObj[key];
        //if (isMongooseObject(fromObj[key]) && !fromObj[key].isMongooseBuffer) {
        //  obj = obj.toObject({ transform: false, virtuals: false });
        //}
        //if (fromObj[key].isMongooseBuffer) {
        //  obj = new Buffer(obj);
        //}
        exports.mergeClone(to[key], obj);
      } else {
        // make sure to retain key order here because of a bug handling the
        // $each operator in mongodb 2.4.4
        to[key] = exports.clone(fromObj[key], {retainKeyOrder: 1});
      }
    }
  }
};

exports.tick = function tick(callback) {
  if (typeof callback !== 'function') {
    return;
  }
  return function() {
    try {
      callback.apply(this, arguments);
    } catch (err) {
      // only nextTick on err to get out of
      // the event loop and avoid state corruption.
      process.nextTick(function() {
        throw err;
      });
    }
  };
};

function PopulateOptions(path, select, match, options, model, subPopulate) {
  this.path = path;
  this.match = match;
  this.select = select;
  this.options = options;
  this.model = model;
  if (typeof subPopulate === 'object') {
    this.populate = subPopulate;
  }
  this._docs = {};
}

// make it compatible with utils.clone
PopulateOptions.prototype.constructor = Object;

// expose
exports.PopulateOptions = PopulateOptions;

exports.populate = function populate(path, select, model, match, options, subPopulate) {
  // The order of select/conditions args is opposite Model.find but
  // necessary to keep backward compatibility (select could be
  // an array, string, or object literal).

  // might have passed an object specifying all arguments
  if (arguments.length === 1) {
    if (path instanceof PopulateOptions) {
      return [path];
    }

    if (Array.isArray(path)) {
      return path.map(function(o) {
        return exports.populate(o)[0];
      });
    }

    if (exports.isObject(path)) {
      match = path.match;
      options = path.options;
      select = path.select;
      model = path.model;
      subPopulate = path.populate;
      path = path.path;
    }
  } else if (typeof model !== 'string' && typeof model !== 'function') {
    options = match;
    match = model;
    model = undefined;
  }

  if (typeof path !== 'string') {
    throw new TypeError('utils.populate: invalid path. Expected string. Got typeof `' + typeof path + '`');
  }

  if (typeof subPopulate === 'object') {
    subPopulate = exports.populate(subPopulate);
  }

  var ret = [];
  var paths = path.split(' ');
  options = exports.clone(options, { retainKeyOrder: true });
  for (var i = 0; i < paths.length; ++i) {
    ret.push(new PopulateOptions(paths[i], select, match, options, model, subPopulate));
  }

  return ret;
};


exports.array = {};

exports.array.flatten = function flatten(arr, filter, ret) {
  ret || (ret = []);

  arr.forEach(function(item) {
    if (Array.isArray(item)) {
      flatten(item, filter, ret);
    } else {
      if (!filter || filter(item)) {
        ret.push(item);
      }
    }
  });

  return ret;
};

exports.array.unique = function(arr) {
  var primitives = {};
  var ids = {};
  var ret = [];
  var length = arr.length;
  for (var i = 0; i < length; ++i) {
    if (typeof arr[i] === 'number' || typeof arr[i] === 'string') {
      if (primitives[arr[i]]) {
        continue;
      }
      ret.push(arr[i]);
      primitives[arr[i]] = true;
    }
    /* TODO
    else if (arr[i] instanceof ObjectId) {
      if (ids[arr[i].toString()]) {
        continue;
      }
      ret.push(arr[i]);
      ids[arr[i].toString()] = true;
    }
    */
    else {
      ret.push(arr[i]);
    }
  }

  return ret;
};


exports.deepEqual = function deepEqual(a, b) {
  if (a === b) {
    return true;
  }

  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }

/* TODO
  if (a instanceof ObjectId && b instanceof ObjectId) {
    return a.toString() === b.toString();
  }
*/

  if (a instanceof RegExp && b instanceof RegExp) {
    return a.source === b.source &&
        a.ignoreCase === b.ignoreCase &&
        a.multiline === b.multiline &&
        a.global === b.global;
  }

  if (typeof a !== 'object' && typeof b !== 'object') {
    return a == b;
  }

  if (a === null || b === null || a === undefined || b === undefined) {
    return false;
  }

  if (a.prototype !== b.prototype) {
    return false;
  }

  // Handle MongooseNumbers
  if (a instanceof Number && b instanceof Number) {
    return a.valueOf() === b.valueOf();
  }

  if (Buffer.isBuffer(a)) {
    return exports.buffer.areEqual(a, b);
  }

  if (isMongooseObject(a)) {
    a = a.toObject();
  }
  if (isMongooseObject(b)) {
    b = b.toObject();
  }

  try {
    var ka = Object.keys(a),
        kb = Object.keys(b),
        key, i;
  } catch (e) {
    // happens when one is a string literal and the other isn't
    return false;
  }

  // having the same number of owned properties (keys incorporates
  // hasOwnProperty)
  if (ka.length !== kb.length) {
    return false;
  }

  // the same set of keys (although not necessarily the same order),
  ka.sort();
  kb.sort();

  // ~~~cheap key test
  for (i = ka.length - 1; i >= 0; i--) {
    if (ka[i] !== kb[i]) {
      return false;
    }
  }

  // equivalent values for every corresponding key, and
  // ~~~possibly expensive deep test
  for (i = ka.length - 1; i >= 0; i--) {
    key = ka[i];
    if (!deepEqual(a[key], b[key])) {
      return false;
    }
  }

  return true;
};
