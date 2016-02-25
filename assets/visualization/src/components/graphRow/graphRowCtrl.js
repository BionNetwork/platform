(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('graphRowCtrl', ['$scope', graphRowCtrl]);

  function graphRowCtrl($scope) {
    $scope.name = $scope.name || "Not given";
  }

})();
