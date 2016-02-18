(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('graphMark', graphMark);

  function graphMark() {
    return {
      scope: { },
      restrict: 'E',
      templateUrl: 'components/graphMark/graphMarkTmpl.html'
    };
  }
})();
