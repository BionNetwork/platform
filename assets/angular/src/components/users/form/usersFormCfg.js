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
      .state('users.add', {
        url: "/add",
        controller: 'usersFormController',
        templateUrl: '/assets/angular/dist/components/users/form/usersFormTmpl.html'
      })
      .state('users.edit', {
        url: "/edit/:id",
        controller: 'usersFormController',
        templateUrl: '/assets/angular/dist/components/users/form/usersFormTmpl.html'
      });

  }

})();

