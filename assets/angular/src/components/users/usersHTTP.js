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
      user.id = users.length + 1;
      users.push(user);
      deferred.resolve(user);
      return deferred.promise;
    };

    this.update = function update(user) {
      var deferred = $q.defer(),
          found = false,
          i, l = users.length;

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
        if (users[i].id == user.id) {
          found = true;
          users[i] = user;
          deferred.resolve(JSON.parse(JSON.stringify(users[i])));
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
          found, i, l = users.length;

      if (criteria) {
        if (criteria.id) {
          found = false;
          for (i = 0; i < l; i++) {
            if (users[i].id == criteria.id) {
              found = true;
              deferred.resolve(JSON.parse(JSON.stringify(users[i])));
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
        deferred.resolve(JSON.parse(JSON.stringify(users)));
      }
      return deferred.promise;
    };

    this.remove = function remove(user) {
      var deferred = $q.defer(),
          found = false,
          i, l = users.length;

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
        if (users[i].id == user.id) {
          found = true;
          users.splice(i, 1);
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
