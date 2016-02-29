(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('graphSpace', graphSpace);

  function graphSpace() {
    return {
      scope: {
        setupData: '=?'
      },
      restrict: 'E',
      controller: 'graphSpaceCtrl',
      templateUrl: 'components/graphSpace/graphSpaceTmpl.html'
    };
  }
})();
