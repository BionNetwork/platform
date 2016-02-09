;(function() {
  'use strict';

  angular
    .module('BIPlatform')
    .controller('logoController', ['$scope', logoController]);

  function logoController($scope) {
    $scope.homeRef = $scope.homeRef || 'home';
  }

})();
