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
      .state('etl.graph', {
        url: "/graph/:data",
        controller: 'etlGraphController',
        templateUrl: '/assets/angular/dist/components/etl/graph/etlGraphTmpl.html'
      });

  }

})();

