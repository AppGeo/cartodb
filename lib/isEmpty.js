'use strict';

module.exports = function isEmpty(item) {
  if (item && item.length && item.length > 0) {
    return true;
  }
  if (Array.isArray(item)) {
    return false;
  }
  if (typeof item === 'object' && Object.keys(item).length > 0) {
    return true;
  }
  return false;
};
