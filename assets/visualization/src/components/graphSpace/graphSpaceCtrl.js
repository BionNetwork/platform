;(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('graphSpaceCtrl', ['$scope', 'graphSpaceFtry', graphSpaceCtrl]);

  function normalize(value) {
    return Number(value);
  }

  function makeMagia($scope) {
    var columns = [],
        column,
        key,
        type,
        i, l,
        j, m;

    // gather only rows
    l = $scope.data.rows.length;
    m = $scope.data1.length;

    for (i = 0; i < l; i++) {
      key = $scope.data.rows[i].name;
      type = $scope.data.rows[i].type;

      column = {
        values: [],
        key: key
      };

      for (j = 0; j < m; j++) {
        column.values.push({
          x: j,
          y: normalize($scope.data1[j][key])
        });
      }
      columns.push(column);
    }
    return columns;
  }

  function graphSpaceCtrl($scope, graphSpaceFtry) {
    $scope.data = [];
    var s = "";

    $scope.setupData1 = function setupData1(data) {
      $scope.data1 = data;
    };

    $scope.setupData = function setupData(data) {
      $scope.data = data;
      if (s === "") {
        s = "render1";
      }
      if (s === "render1") {

        $scope.render1();
      }
      else if (s === "render2") {

        $scope.render2();
      }
      $scope.$digest();  /// I hate you Angular!
    }

    $scope.render1 = function() {
      var graph = graphSpaceFtry.graph1();
      $scope.options = graph.options;
      $scope.data2 = makeMagia($scope);
      s = "render1";
    }

    $scope.render2 = function() {
      var graph = graphSpaceFtry.graph2();
      $scope.options = graph.options;
      $scope.data2 = makeMagia($scope);
      s = "render2";
    }
  }

})();
