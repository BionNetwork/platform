;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('usersViewController', ['$scope', '$usersHTTP', usersViewController]);

  function usersViewController($scope, $usersHTTP) {
    $scope.users = [];
    $scope.currentUser = undefined;

    $usersHTTP.read().then(function(response) {
      $scope.users = response;
    });

    $scope.confirmRemove = function confirmRemove() {
      console.log('confirmRemove item', $scope.currentUser);
    };

    $scope.cancelRemove = function cancelRemove() {
      console.log('cancelRemove item', $scope.currentUser);
    };

    $scope.prepareRemove = function prepareRemove(item) {
      $scope.currentUser = item;
    };
  }
})();
