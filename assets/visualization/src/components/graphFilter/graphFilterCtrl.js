(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('graphFilterCtrl', ['$scope', graphFilterCtrl]);

  function graphFilterCtrl($scope) {
    $scope.name = $scope.name || "Not given";
  }

})();
