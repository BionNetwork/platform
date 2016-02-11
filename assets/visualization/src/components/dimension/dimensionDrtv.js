(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('dimension', dimension);

  function dimension() {
    return {
      scope: { },
      restrict: 'E',
      templateUrl: 'components/dimension/dimensionTmpl.html'
    };
  }
})();
