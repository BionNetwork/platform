(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('measuresCtrl', [
    '$scope',
    '$measures',
    measuresCtrl]);

  function measuresCtrl($scope, $measures) {
    $scope.setupItems = function setupItems(array) {
      $measures.setupItems(array);
      $scope.items = $measures.getItems();
    }
  }

})();
