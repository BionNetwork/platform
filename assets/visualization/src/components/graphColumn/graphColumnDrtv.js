(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('graphColumn', graphColumn);

  function graphColumn() {
    return {
      scope: { },
      restrict: 'E',
      templateUrl: 'components/graphColumn/graphColumnTmpl.html'
    };
  }
})();
