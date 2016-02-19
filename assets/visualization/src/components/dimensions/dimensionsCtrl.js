(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('dimensionsCtrl', [
    '$scope',
    '$dimensions',
    dimensionsCtrl
  ]);

  function dimensionsCtrl($scope, $dimensions) {
    $scope.setupItems = $scope.setupItems || function setupItems(items) {
      $dimensions.setupItems(items);
      $scope.items = $dimensions.getItems();
    };
  }
})();
