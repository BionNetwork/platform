(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('graphRow', graphRow);

  function graphRow() {
    return {
      scope: {
        name: '=?'
      },
      restrict: 'E',
      controller: 'graphRowCtrl',
      templateUrl: 'components/graphRow/graphRowTmpl.html'
    };
  }
})();
