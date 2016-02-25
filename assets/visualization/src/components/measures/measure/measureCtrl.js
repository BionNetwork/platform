(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('measureCtrl', ['$scope', measureCtrl]);

  function measureCtrl($scope) {
    $scope.name = $scope.name || 'Not given';
  }

})();
