(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('cube', cube);

  function cube() {
    return {
      scope: {
        setupMetadata: '=?',
        getDimensions: '=?',
        getMeasures: '=?'
      },
      transclude: true,
      restrict: 'E',
      controller: 'cubeCtrl',
      templateUrl: 'components/cube/cubeTmpl.html'
    };
  }
})();
