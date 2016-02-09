;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .service('$etlHTTP', ['$http', '$q', etlHTTP]);

  function etlHTTP($http, $q) {
    var etl = [
        {
          id: 11,
          username: 'etton',
          email: 'bi@etton.ru',
          status: 'active'
        },
        {
          id: 12,
          username: 'test',
          email: 'rios@etton.ru',
          status: 'nonactive'
        }
      ];

    this.add = function add(user) {
      var deferred = $q.defer();
      user.id = etl.length + 1;
      etl.push(user);
      deferred.resolve(user);
      return deferred.promise;
    };

    this.update = function update(user) {
      var deferred = $q.defer(),
          found = false,
          i, l = etl.length;

      if (user) {
        if (!user.id) {
          deferred.reject({
            message: 'incorrect user - has not id'
          });
          return deferred.promise;
        }
      }
      else {
        deferred.reject({
          message: 'no user to update was provided'
        });
        return deferred.promise;
      }
      for (i = 0; i < l; i++) {
        if (etl[i].id == user.id) {
          found = true;
          etl[i] = user;
          deferred.resolve(JSON.parse(JSON.stringify(etl[i])));
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
          found, i, l = etl.length;

      if (criteria) {
        if (criteria.id) {
          found = false;
          for (i = 0; i < l; i++) {
            if (etl[i].id == criteria.id) {
              found = true;
              deferred.resolve(JSON.parse(JSON.stringify(etl[i])));
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
        deferred.resolve(JSON.parse(JSON.stringify(etl)));
      }
      return deferred.promise;
    };

    this.remove = function remove(user) {
      var deferred = $q.defer(),
          found = false,
          i, l = etl.length;

      if (user) {
        if (!user.id) {
          deferred.reject({
            message: 'incorrect user - has not id'
          });
          return deferred.promise;
        }
      }
      else {
        deferred.reject({
          message: 'no user to update was provided'
        });
        return deferred.promise;
      }
      for (i = 0; i < l; i++) {
        if (etl[i].id == user.id) {
          found = true;
          etl.splice(i, 1);
          deferred.resolve(JSON.parse(JSON.stringify(user)));
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
