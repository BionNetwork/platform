;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlGraphController', ['$scope', '$state', '$etlGraphHTTP', etlGraphController]);

  function etlGraphController($scope, $state, $etlGraphHTTP) {
    var data = JSON.parse($state.params.data);

    function successRead(response) {
      console.log(response, 'jajajajajajaja');
    }

    function errorRead(reason) {
      console.log('reason', reason);
    }

    $etlGraphHTTP
      .requestContent(data)
      .then(successRead, errorRead);
  }
})();
