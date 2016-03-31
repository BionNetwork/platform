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
      .state('etl.manage', {
        url: "/manage/:id",
        controller: 'etlManageController',
        templateUrl: '/assets/angular/dist/components/etl/manage/etlManageTmpl.html'
      });

  }

})();

