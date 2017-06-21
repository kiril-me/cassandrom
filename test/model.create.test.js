var start = require('./common'),
    assert = require('power-assert'),
    cassandrom = start.cassandrom,
    random = require('../lib/utils').random,
    Schema = cassandrom.Schema,
    UUIDType = cassandrom.UUIDType,
    Promise = require('../lib/promise');


var schema = new Schema({
  id: { type: cassandrom.UUIDType, default: cassandrom.uuid },
  title: { type: String, required: true }
}, [ 'id' ]);

var SchemaWithPreSaveHook = new Schema({
  id: { type: cassandrom.UUIDType, default: cassandrom.uuid },
  preference: String
}, [ 'id' ]);

describe('model', function() {
  describe('create()', function() {
    var db;
    var B;

    var db2;
    var MWPSH;

    //this.timeout(15000);

    before(function(done) {
      db = start(function(error, result) {
        B = db.model('model_create', schema, 'model_create_' + random(), function(error) {
          if(error) {
            done(error);
          } else {
            db2 = start(function(error, result) {
              MWPSH = db2.model('mwpsh', SchemaWithPreSaveHook, function(error) {
                done(error);
              });
            });
          }
        });
      });
    });

    after(function(done) {
      db.close(function() {
        db2.close(done);
      });
    });

    it('accepts an array and returns an array', function(done) {
      B.create([{title: 'hi'}, {title: 'bye'}], function(err, posts) {
        assert.ifError(err);
        assert.ok(posts instanceof Array);
        assert.equal(posts.length, 2);
        var post1 = posts[0];
        var post2 = posts[1];

        assert.ok(typeof post1.get('id') === "string");
        assert.equal(post1.title, 'hi');

        assert.ok(typeof post2.get('id') === "string");
        assert.equal(post2.title, 'bye');

        done();
      });
    });

    it('fires callback when passed 0 docs', function(done) {
      B.create(function(err, a) {
        assert.ifError(err);
        assert.ok(!a);
        done();
      });
    });

    it('fires callback when empty array passed', function(done) {
      B.create([], function(err, a) {
        assert.ifError(err);
        assert.ok(!a);
        done();
      });
    });

    it('should not cause unhandled reject promise', function(done) {

      B.create({title: 'reject promise'}, function(err, b) {
        assert.ifError(err);

        var perr = null;
        var p = B.create({id: b.id}, function(err) {
          assert(err);
          setTimeout(function() {
            done(perr);
          }, 100);
        });
        p.catch(function(err) {
          // should not go here
          perr = err;
        });
      });
    });

    it('returns a promise', function(done) {
      var p = B.create({title: 'returns promise'}, function() {
        assert.ok(p instanceof cassandrom.Promise);
        done();
      });
    });

    it('creates in parallel', function(done) {
      // we set the time out to be double that of the validator - 1 (so that running in serial will be greater than that)
      this.timeout(1000);

      after(function(done) {
        db.close(done);
      });

      var countPre = 0,
          countPost = 0;

      SchemaWithPreSaveHook.pre('save', true, function hook(next, done) {
        setTimeout(function() {
          countPre++;
          next();
          done();
        }, 500);
      });
      SchemaWithPreSaveHook.post('save', function() {
        countPost++;
      });

      MWPSH.create([
        {preference: 'xx'},
        {preference: 'yy'},
        {preference: '1'},
        {preference: '2'}
      ], function(err, docs) {

        assert.ifError(err);

        assert.ok(docs instanceof Array);
        assert.equal(docs.length, 4);
        var doc1 = docs[0];
        var doc2 = docs[1];
        var doc3 = docs[2];
        var doc4 = docs[3];
        assert.ok(doc1);
        assert.ok(doc2);
        assert.ok(doc3);
        assert.ok(doc4);
        assert.equal(countPre, 4);
        assert.equal(countPost, 4);
        done();
      });
    });

    describe('callback is optional', function() {
      it('with one doc', function(done) {
        var p = B.create({title: 'optional callback'});
        p.then(function(doc) {
          assert.equal(doc.title, 'optional callback');
          done();
        }, done);
      });

      it('with more than one doc', function(done) {
        var p = B.create({title: 'optional callback 2'}, {title: 'orient expressions'});
        p.then(function(doc1, doc2) {
          assert.equal(doc1.title, 'optional callback 2');
          assert.equal(doc2.title, 'orient expressions');
          done();
        }, done);
      });

      it('with array of docs', function(done) {
        var p = B.create([{title: 'optional callback3'}, {title: '3'}]);
        p.then(function(docs) {
          assert.ok(docs instanceof Array);
          assert.equal(docs.length, 2);
          var doc1 = docs[0];
          var doc2 = docs[1];
          assert.equal(doc1.title, 'optional callback3');
          assert.equal(doc2.title, '3');
          done();
        }, done);
      });

      it('and should reject promise on error', function(done) {
        var p = B.create({title: 'optional callback 4'});
        p.then(function(doc) {
          var p2 = B.create({_id: doc._id});
          p2.then(function() {
            assert(false);
          }, function(err) {
            assert(err);
            done();
          });
        }, done);
      });

      it('if callback is falsy, will ignore it', function(done) {
        B.create({ title: 'test' }, null).
          then(function(doc) {
            assert.equal(doc.title, 'test');
            done();
          }).catch(done);
      });

    });

  });
});

