;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .service('$etlGraphHTTP', ['$http', '$q', etlGraphHTTP]);

  function etlGraphHTTP($http, $q) {

    this.requestContent = function(request) {
      return $http.post(request.url, request.colsInfo, {
        headers: {
          'X-CSRFToken': csrftoken       
        }
      });
    }
  }

})();
