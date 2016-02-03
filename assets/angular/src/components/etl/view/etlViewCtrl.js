;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlViewController', ['$scope', '$etlHTTP', etlViewController]);

  function etlViewController($scope, $etlHTTP) {
    $scope.etl = [];
    $scope.currentUser = undefined;

    function successRead(etl) {
      $scope.etl = etl;
    }

    function successRemove(user) {
      var etl = $scope.etl,
          l = etl.length,
          found = false,
          i;

      for (i = 0; i < l; i++) {
        if (etl[i].id == user.id) {
          found = true;
          etl.splice(i, 1);
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

    $etlHTTP
      .read()
      .then(successRead, errorHandler);

    $scope.confirmRemove = function confirmRemove() {
      $etlHTTP
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
