(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('graphFilter', graphFilter);

  function graphFilter() {
    return {
      scope: {
        name: '=?'
      },
      restrict: 'E',
      controller: 'graphFilterCtrl',
      templateUrl: 'components/graphFilter/graphFilterTmpl.html'
    };
  }
})();
