'use strict';

module.exports = function assign(host, other) {
  Object.keys(other).forEach(function (key) {
    host[key] = other[key];
  });
  return host;
};
