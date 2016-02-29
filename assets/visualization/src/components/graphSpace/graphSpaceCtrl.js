(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('graphSpaceCtrl', ['$scope', graphSpaceCtrl]);

  function graphSpaceCtrl($scope) {
    $scope.data = [];

    $scope.setupData = function setupData(data) {
      console.log(data);
      $scope.data = data;
    }
  }

})();
