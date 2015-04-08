var SchemaType = require('../schematype')
  , CastError = SchemaType.CastError
  , utils = require('../utils'),
  Types = {
      Boolean: require('./boolean')
    , Date: require('./date')
    , Number: require('./number')
    , String: require('./string')
    , UUIDType: require('./uuid')
  };

function SchemaCollection (key, cast, options) {
  if (cast) {
    var castOptions = {};

    if ('Object' === cast.constructor.name) {
      if (cast.type) {
        // support { type: Woot }
        castOptions = utils.clone(cast); // do not alter user arguments
        delete castOptions.type;
        cast = cast.type;
      } else {
        cast = Mixed;
      }
    }

    var name = 'string' == typeof cast
      ? cast
      : cast.name;

    var caster = name in Types
      ? Types[name]
      : cast;

    this.casterConstructor = caster;
    this.caster = new caster(null, castOptions);
  }

  SchemaType.call(this, key, options);

  var self = this
    , defaultArr
    , fn;

  if (this.defaultValue) {
    defaultArr = this.defaultValue;
    fn = 'function' == typeof defaultArr;
  }
}

SchemaCollection.prototype.__proto__ = SchemaType.prototype;


module.exports = SchemaCollection;