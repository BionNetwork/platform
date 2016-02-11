(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('graphFilter', graphFilter);

  function graphFilter() {
    return {
      scope: { },
      restrict: 'E',
      templateUrl: 'components/graphFilter/graphFilterTmpl.html'
    };
  }
})();
