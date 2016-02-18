(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('shelf', shelf);

  function shelf() {
    return {
      scope: { },
      restrict: 'E',
      transclude: true,
      templateUrl: 'shared/shelf/shelfTmpl.html'
    };
  }
})();
