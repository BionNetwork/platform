;(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .controller('shelfCtrl', ['$scope', shelfCtrl]);

  var lastId = 1,
      UNKNOWN_TYPE = 'unknown';

  function checkType($scope) {
    if ($scope.type === UNKNOWN_TYPE) {
      console.error('shelfCtrl can not process call');
      return false;
    }
    return true;
  }

  function shelfCtrl($scope) {
    $scope.items = $scope.items || [];
    $scope.type = $scope.type || UNKNOWN_TYPE;

    $scope.onAddItem = $scope.onAddItem || function onAddItem(item) { };
    $scope.onRemoveItem = $scope.onRemoveItem || function onRemoveItem(item) { };

    $scope.resetLastId = function resetLastId() {
      lastId = 1;
    };

    $scope.addItem = function addItem(item) {
      if (!checkType($scope)) { return false; }
      item._id = lastId++;
      $scope.onAddItem(item);
      $scope.items.push(item);
    };

    $scope.getItemBy_id = function getItemBy_id(_id) {
      if (!checkType($scope)) { return false; }
      var i, l = $scope.items.length;
      for (i = 0; i < l; i++) {
        if ($scope.items[i]._id === _id) {
          return $scope.items[i];
        }
      }
      return null;
    };

    $scope.removeItem = function removeItem(item) {
      if (!checkType($scope)) { return false; }
      var i, l = $scope.items.length;
      for (i = 0; i < l; i++) {
        if ($scope.items[i]._id === item._id) {
          $scope.onRemoveItem($scope.items[i]);
          $scope.items.splice(i, 1);
          break;
        }
      }
    };
  }

})();
