(function() {
  'use strict';
  angular
  .module('BI-visualization')
  .service('$cube', cubeSrvc);

  var continuosType = [
    'FLOAT',
    'INTEGER',
    'DOUBLE',
    'NUMBER'
  ];

  function cubeSrvc() {
    var _metadata = null,
        _dimensions = null,
        _measures = null;

    this.setupMetadata = function setupMetadata(metadata) {
      _metadata = metadata;
    };

    this.getMetadata = function getMetadata() {
      return _metadata;
    };

    this.analyseMetadata = function analyseMetadata() {
      var type;
      _dimensions = [];
      _measures = [];

      _metadata.forEach(function(item) {
        type = item.type.toUpperCase();
        if (continuosType.indexOf(type) != -1) {
          _measures.push(item);
        }
        else {
          _dimensions.push(item);
        }
      });
    };

    this.getDimensions = function getDimensions() {
      return _dimensions;
    };

    this.getMeasures = function getMeasures() {
      return _measures;
    };
  }

})();
