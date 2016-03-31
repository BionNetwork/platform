;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .config([
      '$stateProvider',
      '$urlRouterProvider',
      router
    ]);

  function router($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('not-found', {
        url: "/not-found",
        template: "not found"
      })
      .state('home', {
        url: "/",
        templateUrl: '/assets/angular/dist/main/mainTmpl.html',
        controller: "mainCtrl"
      });

    $urlRouterProvider.otherwise(function($injector, $location) {
      if ($location.path() === '') {
        return '/';
      }
      //return '/not-found';
    });
  }
})();
