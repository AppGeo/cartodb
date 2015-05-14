'use strict';

module.exports = function assign(host, other) {
  if (!other) {
    return host;
  }
  Object.keys(other).forEach(function (key) {
    host[key] = other[key];
  });
  return host;
};
