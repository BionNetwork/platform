(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .service('$dimensions', dimensionsSrvc);

  var continuosType = [
    'FLOAT',
    'INTEGER',
    'DOUBLE',
    'NUMBER'
  ];

  function dimensionsSrvc() {

    var _items = null;
    this.setupItems = function setupItems(items) {
      _items = items;
    };

    this.getItems = function getItems() {
      return _items;
    };

  }

})();
