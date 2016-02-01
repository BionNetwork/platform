;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .service('$usersHTTP', ['$http', '$q', usersHTTP]);

  function usersHTTP($http, $q) {
    var users = [
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
      users.push(user);
      deferred.resolve(user);
      return deferred.promise;
    };

    this.update = function update(user) {
      var deferred = $q.defer(),
          found = false;

      users.forEach(function(_user) {
        if (_user.id == user.id) {
          found = true;
          _user = JSON.parse(JSON.stringify(user));
          deferred.resolve(_user);
        }
      });
      if (!found) {
        deferred.reject({
          message: 'cannot update'
        });
      }
     return deferred.promise;
    };

    this.read = function read(criteria) {
      var deferred = $q.defer(),
          found;

      if (criteria) {
        if (criteria.id) {
          found = false;
          users.forEach(function(user) {
            if (user.id == criteria.id) {
              found = true;
              deferred.resolve(user);
            }
          });
          if (!found) {
            deferred.reject({
              message: 'not found'
            });
          }
        }
      }
      else {
        deferred.resolve(users);
      }
      return deferred.promise;
    };

    this.remove = function remove(user) {

    };

  }

})();
