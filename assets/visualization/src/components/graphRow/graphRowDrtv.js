(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('graphRow', graphRow);

  function graphRow() {
    return {
      scope: { },
      restrict: 'E',
      templateUrl: 'components/graphRow/graphRowTmpl.html'
    };
  }
})();
