'use strict';

module.exports = function isEmpty(item) {
  if (item === null || item === void 0) {
    return true;
  }
  if (typeof item.length === 'number' && item.length > -1) {
    return !item.length;
  }
  return !Object.keys(item).length;
};
