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
      .state('etl.view', {
        url: "/view",
        controller: 'etlViewController',
        templateUrl: '/assets/angular/dist/components/etl/view/etlViewTmpl.html'
      });
  }
})();

