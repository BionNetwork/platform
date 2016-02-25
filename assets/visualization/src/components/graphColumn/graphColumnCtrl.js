(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('graphColumnCtrl', ['$scope', graphColumnCtrl]);

  function graphColumnCtrl($scope) {
    $scope.name = $scope.name || "Not given";
  }

})();
