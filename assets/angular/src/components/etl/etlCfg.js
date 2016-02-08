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
      .state('etl', {
        abstract: true,
        url: "/etl",
        controller: 'etlController',
        templateUrl: '/assets/angular/dist/components/etl/etlTmpl.html'
      });
  }

})();

