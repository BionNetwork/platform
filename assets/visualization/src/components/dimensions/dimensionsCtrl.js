;(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('dimensionsCtrl', [
    '$scope',
    '$dimensions',
    dimensionsCtrl
  ]);

  function dimensionsCtrl($scope, $dimensions) {
    $scope.items = $scope.items || [];
    $scope.setupItems = $scope.setupItems || function setupItems(items) {
      $dimensions.setupItems(items);
      $scope.items = $dimensions.getItems();
    };

    $scope.getItemByName = function getItemByName(name) {
      var i, l = $scope.items.length;
      for (i = 0; i < l; i++) {
        if ($scope.items[i].name === name) {
          return $scope.items[i];
        }
      }
      return null;
    };
  }
})();
