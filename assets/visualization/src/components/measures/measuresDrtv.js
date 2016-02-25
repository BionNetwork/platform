(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('measures', measures);

  function measures() {
    return {
      scope: {
        items: '=?',
        setupItems: '=?'
      },
      restrict: 'E',
      controller: 'measuresCtrl',
      templateUrl: 'components/measures/measuresTmpl.html'
    };
  }
})();
