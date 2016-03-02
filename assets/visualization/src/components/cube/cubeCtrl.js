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

    var graphSpaceState = {
      filters: [],
      columns: [],
      rows: []
    };

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
      graphSpaceState.filters.push(item);
      fnGraphData(graphSpaceState);
    };

    $scope.onAddGraphRow = function onAddGraphRow(item) {
      graphSpaceState.rows.push(item);
      fnGraphData(graphSpaceState);
    };

    $scope.onAddGraphColumn = function onAddGraphColumn(item) {
      graphSpaceState.columns.push(item);
      fnGraphData(graphSpaceState);
    };
  }
})();
