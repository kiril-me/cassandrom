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