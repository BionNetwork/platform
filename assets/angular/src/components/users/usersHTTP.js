;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .service('$usersHTTP', ['$http', '$q', usersHTTP]);

  function usersHTTP($http, $q) {
    var users = [
        {
          id: 11,
          user: 'etton',
          email: 'bi@etton.ru',
          status: 'active'
        },
        {
          id: 12,
          user: 'test',
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

    this.update = function update() {

    };

    this.read = function read() {
      var deferred = $q.defer();
      deferred.resolve(users);
      return deferred.promise;
    };

    this.remove = function remove() {

    };

  }

})();
