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

    var fnGraphData = function dummyGraphData() {
      console.log('calling dummy');
    };

    $scope.setupMetadata = function setupMetadata(metadata) {
      $cube.setupMetadata(metadata);
      $cube.analyseMetadata();
    };

    $scope.getDimensions = function getDimensions() {
      return $cube.getDimensions();
    };

    $scope.getMeasures = function getMeasures() {
      return $cube.getMeasures();
    };

    $scope.registerGraphData = function registerGraphData(fn) {
      fnGraphData = fn;
    };

    $scope.onAddGraphFilter = function onAddGraphFilter(item) {
      var grph = [1, 2, 3, 4, 5, 6];
      fnGraphData(grph);
    };

    $scope.onAddGraphRow = function onAddGraphRow(item) {
      var grph = [9, 8, 5, 4, 5, 2];
      fnGraphData(grph);
    };

    $scope.onAddGraphColumn = function onAddGraphColumn(item) {
      var grph = [6, 7, 8, 9, 0, 1];
      fnGraphData(grph);
    };
  }
})();
