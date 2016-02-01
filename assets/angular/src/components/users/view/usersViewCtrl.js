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

    function successRemove(user) {
      var users = $scope.users,
          l = users.length,
          found = false,
          i;

      for (i = 0; i < l; i++) {
        if (users[i].id == user.id) {
          found = true;
          users.splice(i, 1);
          break;
        }
      }

      if (found) {
        $('#userRemoveModal').modal('hide');
      }
      else {
        console.log('Something went wrong...');
      }
    }

    function errorHandler(reason) {
      console.log('error', reason);
    }

    $usersHTTP
      .read()
      .then(successRead, errorHandler);

    $scope.confirmRemove = function confirmRemove() {
      $usersHTTP
        .remove($scope.currentUser)
        .then(successRemove, errorHandler);
    };

    $scope.cancelRemove = function cancelRemove() {
      console.log('cancelRemove item', $scope.currentUser);
    };

    $scope.prepareRemove = function prepareRemove(item) {
      $scope.currentUser = item;
    };
  }
})();
