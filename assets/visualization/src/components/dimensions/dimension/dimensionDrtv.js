(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('dimension', dimension);

  function dimension() {
    return {
      scope: {
        name: '='
      },
      restrict: 'E',
      templateUrl: 'components/dimensions/dimension/dimensionTmpl.html'
    };
  }
})();
