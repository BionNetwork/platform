;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('usersFormController', ['$scope', '$state', '$usersHTTP', usersFormController]);

  function usersFormController($scope, $state, $usersHTTP) {
    $scope.user = {
      status: 'active'
    };

    function successRead(user) {
      $scope.user = user;
    }

    function successCreate(user) {
      $state.go('users.view');
    }

    function successUpdate(user) {
      $state.go('users.view');
    }

    function errorHandler(reason) {
      console.log('error', reason);
    }

    if ($state.params.id) {
      $usersHTTP.read({
        id: $state.params.id
      }).then(successRead, errorHandler);
    }

    $scope.submit = function submit(user) {
      if (user.id) {
        $usersHTTP
          .update(user)
          .then(successUpdate, errorHandler);
      }
      else {
        $usersHTTP
          .add(user)
          .then(successCreate, errorHandler);
      }
    };
  }
})();
