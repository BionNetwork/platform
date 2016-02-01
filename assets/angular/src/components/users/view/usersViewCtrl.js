;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('usersViewController', ['$scope', '$usersHTTP', usersViewController]);

  function usersViewController($scope, $usersHTTP) {
    $scope.users = [];
    $scope.currentUser = undefined;

    function successRead(users) {
      $scope.users = users;
    }

    function errorHandler(reason) {
      console.log('error', reason);
    }

    $usersHTTP
      .read()
      .then(successRead, errorHandler);

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
