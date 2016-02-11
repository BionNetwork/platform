(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('measure', measure);

  function measure() {
    return {
      scope: { },
      restrict: 'E',
      templateUrl: 'components/measure/measureTmpl.html'
    };
  }
})();
