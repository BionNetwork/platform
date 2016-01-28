
(function sharedLogo() {
  'use strict';
  var bi = angular.module('BIPlatform');

  bi.directive('logo', logo);

  function logo() {
    return {
      restrict: 'E',
      scope: {
        
      },
      templateUrl: '/assets/angular/dist/shared/logo/logoTmpl.html'
    };
  }
})();
