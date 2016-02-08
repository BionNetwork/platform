
(function sharedLogo() {
  'use strict';
  angular
    .module('BIPlatform')
    .directive('menu', menu);

  function menu() {
    return {
      restrict: 'E',
      scope: {
      },
      controller: 'menuController',
      templateUrl: '/assets/angular/dist/shared/menu/menuTmpl.html'
    };
  }
})();
