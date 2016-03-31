;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlManageController', ['$scope', '$state', '$etlManageHTTP', etlManageController]);

  function etlManageController($scope, $state, $etlManageHTTP) {
    var id = $state.params.id;

    $scope.getRequest_RefreshData = function() {
      $state.go('etl.graph', { data: JSON.stringify(window.refreshData_request) });
    };
    getConnectionData('/etl/datasources/get_data/' + id + '/', '/etl/datasources/remove_all_tables/');
  }
})();
