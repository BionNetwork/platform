
(function sharedLogo() {
  'use strict';
  angular
    .module('BIPlatform')
    .directive('logo', logo);

  function logo() {
    return {
      restrict: 'E',
      scope: {
        homeRef: '='
      },
      controller: 'logoController',
      templateUrl: '/assets/angular/dist/shared/logo/logoTmpl.html'
    };
  }
})();
