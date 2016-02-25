;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlViewController', ['$scope', '$etlHTTP', etlViewController]);

  function etlViewController($scope, $etlHTTP) {
    $scope.etls = [];
    $scope.currentEtl = undefined;

    function successRead(etls) {
      $scope.etls = etls;
    }

    function successRemove(etl) {
      var etls = $scope.etls,
          l = etls.length,
          found = false,
          i;

      for (i = 0; i < l; i++) {
        if (etls[i].id == etl.id) {
          found = true;
          etls.splice(i, 1);
          break;
        }
      }

      if (found) {
        $('#etlRemoveModal').modal('hide');
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
        .remove($scope.currentEtl)
        .then(successRemove, errorHandler);
    };

    $scope.cancelRemove = function cancelRemove() {
      console.log('cancelRemove item', $scope.currentEtl);
    };

    $scope.prepareRemove = function prepareRemove(item) {
      $scope.currentEtl = item;
    };
  }
})();
