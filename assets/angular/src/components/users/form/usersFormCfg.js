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
      .state('users_add', {
        url: "/users_add",
        controller: 'usersFormController',
        templateUrl: '/assets/angular/dist/components/users/form/usersFormTmpl.html'
      });
  }

})();

