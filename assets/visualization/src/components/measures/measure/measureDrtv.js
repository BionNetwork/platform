(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('measure', measure);

  function measure() {
    return {
      scope: {
        name: '='
      },
      restrict: 'E',
      templateUrl: 'components/measures/measure/measureTmpl.html'
    };
  }
})();
