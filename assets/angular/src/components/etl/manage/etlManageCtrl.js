;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlManageController', ['$scope', '$state', '$etlManageHTTP', etlManageController]);

  function etlManageController($scope, $state, $etlManageHTTP) {
    var id = $state.params.id;

    getConnectionData('/etl/datasources/get_data/' + id + '/', '/etl/datasources/remove_all_tables/');
  }
})();
