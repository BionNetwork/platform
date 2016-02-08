;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlFormController', ['$scope', '$state', '$etlHTTP', etlFormController]);

  function etlFormController($scope, $state, $etlHTTP) {
    $scope.user = {
      status: 'active'
    };

    function successRead(user) {
      $scope.user = user;
    }

    function successCreate(user) {
      $state.go('etl.view');
    }

    function successUpdate(user) {
      $state.go('etl.view');
    }

    function errorHandler(reason) {
      console.log('error', reason);
    }

    if ($state.params.id) {
      $etlHTTP.read({
        id: $state.params.id
      }).then(successRead, errorHandler);
    }

    $scope.submit = function submit(user) {
      if (user.id) {
        $etlHTTP
          .update(user)
          .then(successUpdate, errorHandler);
      }
      else {
        $etlHTTP
          .add(user)
          .then(successCreate, errorHandler);
      }
    };
  }
})();
