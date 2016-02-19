(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .service('$measures', measuresSrvc);

  function measuresSrvc() {

    var _items = null;
    this.setupItems = function setupItems(items) {
      _items = items;
    };

    this.getItems = function getItems() {
      return _items;
    };

  }

})();
