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
    $scope.items = $scope.items || [];
    $scope.setupItems = function _setupItems(items) {
      $dimensions.setupItems(items);
      $scope.items = $dimensions.getItems();
    };
  }

})();
