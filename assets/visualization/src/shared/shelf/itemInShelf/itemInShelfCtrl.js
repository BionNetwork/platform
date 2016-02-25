(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('itemInShelfCtrl', ['$scope', itemInShelfCtrl]);

  function itemInShelfCtrl($scope) {
    $scope.name = $scope.name || 'Not Given';
    $scope.tag = $scope.tag || 'Not Given';
  }

})();
