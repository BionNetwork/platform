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
        var _item = JSON.parse(JSON.stringify(item));
        type = _item.type.toUpperCase();
        if (continuosType.indexOf(type) != -1) {
          _item.role = "measure";
          _measures.push(_item);
        }
        else {
          _item.role = "dimension";
          _dimensions.push(_item);
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
