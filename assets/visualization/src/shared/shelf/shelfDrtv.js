;(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('shelf', shelf);

  function link(scope, element, attrs, controller, transcludeFn) {
    [].forEach.call(element, function(item) {
      item.addEventListener('dragover', function(e) {
        e.preventDefault();
        e.stopPropagation();
      });
      item.addEventListener('drop', function(e) {
        console.log("drop finished", e.dataTransfer.getData('text/plain'), e);
      }, false);
    });
  }

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
      templateUrl: 'shared/shelf/shelfTmpl.html',
      link: link
    };
  }
})();
