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
      .state('users.view', {
        url: "/view",
        controller: 'usersViewController',
        templateUrl: '/assets/angular/dist/components/users/view/usersViewTmpl.html'
      });
  }
})();

