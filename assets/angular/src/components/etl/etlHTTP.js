;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .service('$etlHTTP', ['$http', '$q', etlHTTP]);

  function etlHTTP($http, $q) {
    var etls = [
        {
          id: 1,
          db: 'biplatform',
          login: 'biplatform',
          password: 'biplatform',
          host: 'localhost',
          port: '5432'
        },
        {
          id: 2,
          db: 'slrm',
          login: 'slrm',
          password: 'slrm',
          host: 'localhost',
          port: '5432'
        }
      ];

    this.add = function add(etl) {
      var deferred = $q.defer();
      etl.id = etls.length + 1;
      etls.push(etl);
      deferred.resolve(etl);
      return deferred.promise;
    };

    this.update = function update(etl) {
      var deferred = $q.defer(),
          found = false,
          i, l = etls.length;

      if (etl) {
        if (!etl.id) {
          deferred.reject({
            message: 'incorrect etl - has not id'
          });
          return deferred.promise;
        }
      }
      else {
        deferred.reject({
          message: 'no etl to update was provided'
        });
        return deferred.promise;
      }
      for (i = 0; i < l; i++) {
        if (etls[i].id == etl.id) {
          found = true;
          etls[i] = etl;
          deferred.resolve(JSON.parse(JSON.stringify(etls[i])));
          break;
        }
      };
      if (!found) {
        deferred.reject({
          message: 'cannot update'
        });
      }
     return deferred.promise;
    };

    this.read = function read(criteria) {
      var deferred = $q.defer(),
          found, i, l = etls.length;

      if (criteria) {
        if (criteria.id) {
          found = false;
          for (i = 0; i < l; i++) {
            if (etls[i].id == criteria.id) {
              found = true;
              deferred.resolve(JSON.parse(JSON.stringify(etls[i])));
              break;
            }
          };
          if (!found) {
            deferred.reject({
              message: 'cannot read'
            });
          }
        }
      }
      else {
        deferred.resolve(JSON.parse(JSON.stringify(etls)));
      }
      return deferred.promise;
    };

    this.remove = function remove(etl) {
      var deferred = $q.defer(),
          found = false,
          i, l = etls.length;

      if (etl) {
        if (!etl.id) {
          deferred.reject({
            message: 'incorrect etl - has not id'
          });
          return deferred.promise;
        }
      }
      else {
        deferred.reject({
          message: 'no etl to update was provided'
        });
        return deferred.promise;
      }
      for (i = 0; i < l; i++) {
        if (etls[i].id == etl.id) {
          found = true;
          etls.splice(i, 1);
          deferred.resolve(JSON.parse(JSON.stringify(etl)));
          break;
        }
      }
      if (!found) {
        deferred.reject({
          message: 'cannot delete'
        });
      }
      return deferred.promise;
    };
  }

})();
