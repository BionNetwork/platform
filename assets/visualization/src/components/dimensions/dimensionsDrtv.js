(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('dimensions', dimensions);

  function dimensions() {
    return {
      scope: {
        items: '='
      },
      restrict: 'E',
      templateUrl: 'components/dimensions/dimensionsTmpl.html'
    };
  }
})();
