;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('usersFormController', ['$scope', '$state', '$usersHTTP', usersFormController]);

  function usersFormController($scope, $state, $usersHTTP) {
    $scope.user = {
      status: 'active'
    };

    $scope.submit = function submit(user) {
      $usersHTTP.add(user).then(function(response) {
        $state.go('users.view');
      });
    };
  }
})();
