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
        controller: 'usersController',
        templateUrl: '/assets/angular/dist/components/users/usersTmpl.html'
      });
  }

})();

