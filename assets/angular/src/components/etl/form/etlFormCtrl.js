;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlFormController', ['$scope', '$state', '$etlHTTP', etlFormController]);

  function etlFormController($scope, $state, $etlHTTP) {
    $scope.etl = {
    };

    function successRead(etl) {
      $scope.etl = etl;
    }

    function successCreate(etl) {
      $state.go('etl.view');
    }

    function successUpdate(etl) {
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

    $scope.submit = function submit(etl) {
      if (etl.id) {
        $etlHTTP
          .update(etl)
          .then(successUpdate, errorHandler);
      }
      else {
        $etlHTTP
          .add(etl)
          .then(successCreate, errorHandler);
      }
    };
  }
})();
