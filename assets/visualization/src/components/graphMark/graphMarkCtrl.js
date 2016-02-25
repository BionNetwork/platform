(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('graphMarkCtrl', ['$scope', graphMarkCtrl]);

  function graphMarkCtrl($scope) {
    $scope.name = $scope.name || "Not given";
  }

})();
