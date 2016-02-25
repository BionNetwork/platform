;(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('shelf', shelf);

  function shelf() {
    return {
      scope: {
        items: '=?',
        type: '=?',
        addItem: '=?', onAddItem: '=?',
        removeItem: '=?', onRemoveItem: '=?'
      },
      restrict: 'E',
      transclude: true,
      controller: 'shelfCtrl',
      templateUrl: 'shared/shelf/shelfTmpl.html'
    };
  }
})();
