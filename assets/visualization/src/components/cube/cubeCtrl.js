(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('cubeCtrl', [
    '$scope',
    '$cube',
    cubeCtrl
  ]);

  function cubeCtrl($scope, $cube) {

    $scope.setupMetadata = function setupMetadata(metadata) {
      $cube.setupMetadata(metadata);
      $cube.analyseMetadata();
    };

    $scope.getDimensions = function getDimensions() {
      return $cube.getDimensions();
    };

    $scope.getMeasures= function getMeasures() {
      return $cube.getMeasures();
    };

  }
})();
