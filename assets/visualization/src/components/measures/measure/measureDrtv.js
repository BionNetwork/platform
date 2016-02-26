(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .directive('measure', measure);

  function link(scope, element, attrs, controller, transcludeFn) {
    [].forEach.call(element, function(item) {
      item.addEventListener('dragstart', function(e) {
        var data = { name: scope.name, type: scope.type, role: scope.role };
        e.dataTransfer.setData('text/plain', JSON.stringify(data));
        e.dataTransfer.effectAllowed = "all";
      }, true);
      item.addEventListener('dragend', function(e) {
        e.preventDefault();
      }, true);
    });
  }

  function measure() {
    return {
      scope: {
        name: '=?',
        type: '=?',
        role: '=?'
      },
      restrict: 'E',
      controller: 'measureCtrl',
      templateUrl: 'components/measures/measure/measureTmpl.html',
      link: link
    };
  }
})();
