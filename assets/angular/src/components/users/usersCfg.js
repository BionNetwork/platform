;(function() {
  "use strict";

  angular
    .module('BIPlatform')
    .config([
      '$stateProvider',
      '$urlRouterProvider',
      route
    ]);

  function route($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('users', {
        url: "/users",
        template: 'Here should be the user\'s table'
      });
  }

})();

