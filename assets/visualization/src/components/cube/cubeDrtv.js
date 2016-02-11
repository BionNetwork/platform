(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('cube', cube);

  function cube() {
    return {
      scope: { },
      restrict: 'E',
      templateUrl: 'components/cube/cubeTmpl.html'
    };
  }
})();
