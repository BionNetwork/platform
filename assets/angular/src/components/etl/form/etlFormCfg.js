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
      .state('etl.add', {
        url: "/add",
        controller: 'etlFormController',
        templateUrl: '/assets/angular/dist/components/etl/form/etlFormTmpl.html'
      })
      .state('etl.edit', {
        url: "/edit/:id",
        controller: 'etlFormController',
        templateUrl: '/assets/angular/dist/components/etl/form/etlFormTmpl.html'
      });

  }

})();

