;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('usersViewController', ['$scope', usersViewController]);

  function usersViewController($scope) {
    $scope.users = [
      {
        id: 11,
        user: 'etton',
        email: 'bi@etton.ru',
        status: 'active'
      },
      {
        id: 12,
        user: 'test',
        email: 'rios@etton.ru',
        status: 'nonactive'
      }
    ];
  }
})();
