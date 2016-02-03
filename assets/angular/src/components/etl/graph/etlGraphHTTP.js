;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .service('$etlGraphHTTP', ['$http', '$q', etlGraphHTTP]);

  function etlGraphHTTP($http, $q) {

    this.requestContent = function(request) {
      var request_ = JSON.parse(JSON.stringify(request.colsInfo));
      return $http.post(request.url, request_, {
        transformRequest: function(obj) {
          var str = [];
          for(var p in obj) {
            str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));
          }
          return str.join("&");
        },
        headers: {
          'X-CSRFToken': csrftoken,
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      });
    }
  }

})();
