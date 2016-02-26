;(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('shelf', shelf);

  function link(scope, element, attrs, controller, transcludeFn) {
    [].forEach.call(element, function(item) {
      item.addEventListener('dragover', function(e) {
        e.preventDefault();
      }, true);
      item.addEventListener('dragenter', function(e) {
        e.preventDefault();
      }, true);
      item.addEventListener('drop', function(e) {
        scope.addItem(JSON.parse(e.dataTransfer.getData('text/plain')));
        scope.$digest();    // I hate you, Angular!!
        e.preventDefault();
      }, true);
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
