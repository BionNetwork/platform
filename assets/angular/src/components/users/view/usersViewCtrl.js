;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('usersViewController', ['$scope', '$usersHTTP', usersViewController]);

  function usersViewController($scope, $usersHTTP) {
    $scope.users = [];
    $usersHTTP.read().then(function(response) {
      $scope.users = response;
    });
  }
})();
