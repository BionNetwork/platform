(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('dimensionCtrl', ['$scope', dimensionCtrl]);

  function dimensionCtrl($scope) {
    $scope.name = $scope.name || 'Not Given';
  }

})();
