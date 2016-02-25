(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('graphMark', graphMark);

  function graphMark() {
    return {
      scope: {
        name: '=?'
      },
      restrict: 'E',
      controller: 'graphMarkCtrl',
      templateUrl: 'components/graphMark/graphMarkTmpl.html'
    };
  }
})();
