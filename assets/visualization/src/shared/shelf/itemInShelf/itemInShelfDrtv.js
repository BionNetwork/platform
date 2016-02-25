(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('itemInShelf', itemInShelf);

  function itemInShelf() {
    return {
      scope: {
        name: '=',
        tag: '='
      },
      restrict: 'E',
      templateUrl: 'shared/shelf/itemInShelf/itemInShelfTmpl.html'
    };
  }
})();
