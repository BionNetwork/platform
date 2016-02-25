(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('graphColumn', graphColumn);

  function graphColumn() {
    return {
      scope: {
        name: '=?'
      },
      restrict: 'E',
      controller: 'graphColumnCtrl',
      templateUrl: 'components/graphColumn/graphColumnTmpl.html'
    };
  }
})();
