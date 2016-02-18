(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('cube', cube);

  function cube() {
    return {
      scope: { },
      transclude: true,
      restrict: 'E',
      templateUrl: 'components/cube/cubeTmpl.html'
    };
  }
})();
