var sliced = require('sliced');
var mpath = require('mpath');

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


