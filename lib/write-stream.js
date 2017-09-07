'use strict';
var debugStream = require('debug')('cartodb:stream');
var stream = require('readable-stream');
var Transform = stream.Transform;
var Promise = require('bluebird');
var duplexify = require('duplexify').obj;
module.exports = function (cartodb) {
  return createWriteStream;
  function createWriteStream(table, opts) {
    opts = opts || {};
    var created = !opts.create;
    var queue = [];
    var max = opts.batchSize || 50;
    var maxInProgress = opts.maxInProgress || 10;

    function mabyeCreate(chunk) {
      if (created) {
        return Promise.resolve(true);
      }
      return cartodb.schema.createTable(table, function (table) {
        Object.keys(chunk.properties).forEach(function (key) {
          switch(typeof chunk.properties[key]) {
            case 'number':
              return table.float(key);
            case 'boolean':
              return table.bool(key);
            default:
              if (chunk.properties[key] instanceof Date) {
                return table.timestamp(key, true);
              }
              return table.text(key);
          }
        });
      }).then(function (a) {
        created = true;
        return a;
      });
    }
    var transform = new Transform({
      objectMode: true,
      transform: function (chunk, _, next) {
        var self = this;
        mabyeCreate(chunk).then(function () {
          queue.push(fixGeoJSON(chunk));
          if (queue.length >= max) {
            var currentQueue = queue;
            queue = [];
            debugStream('queue');
            self.push(currentQueue);
            next();
          } else {
            next();
          }
        }).catch(next);
      },
      flush: function (done) {
        debugStream('flush');
        if (queue.length) {
          this.push(queue);
        }
        done();
      }
    });
    var dup;
    var inProgress = 0;
    function maybeNext(next) {
      if (inProgress > maxInProgress) {
        return dup.once('inserted', function () {
          maybeNext(next);
        });
      }
      next();
    }
    var writable = new stream.Writable({
      objectMode: true,
      write: function (chunk, _, next) {
        debugStream('write');
        inProgress++;
        maybeNext(next);
        cartodb(table).insert(chunk).exec(function (err) {
          if (err) {
            dup.emit('error', err);
          }
          inProgress--;
          dup.emit('inserted', chunk.length);
        });
      },
      final: function (done) {
        if (!inProgress) {
          dup.emit('uploaded');
          done();
        } else {
          dup.on('inserted', function () {
            if (!inProgress) {
              dup.emit('uploaded');
              done();
            }
          });
        }
      }
    });
    transform.pipe(writable);
    dup = duplexify(transform, writable);
    return dup;
  }
};
function fixGeoJSON(chunk) {
  var out = {};
  Object.keys(chunk.properties).forEach(function (key) {
    out[key] = chunk.properties[key];
  });
  out.the_geom = chunk.geometry;
  return out;
}
