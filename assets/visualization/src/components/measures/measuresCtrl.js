(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('measuresCtrl', [
    '$scope',
    '$measures',
    measuresCtrl
  ]);

  function measuresCtrl($scope, $measures) {
    $scope.items = $scope.items || [];
    $scope.setupItems = function setupItems(items) {
      $measures.setupItems(items);
      $scope.items = $measures.getItems();
    }
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
