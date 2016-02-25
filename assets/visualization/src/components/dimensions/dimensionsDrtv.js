;(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('dimensions', dimensions);

  function dimensions() {
    return {
      scope: {
        items: '=?',
        setupItems: '=?'
      },
      restrict: 'E',
      templateUrl: 'components/dimensions/dimensionsTmpl.html',
      controller: 'dimensionsCtrl'
    };
  }
})();
