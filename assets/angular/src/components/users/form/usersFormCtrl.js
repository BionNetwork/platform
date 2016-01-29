;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('usersFormController', ['$scope', usersFormController]);

  function usersFormController($scope) {
    console.log('users form controller');
  }
})();
