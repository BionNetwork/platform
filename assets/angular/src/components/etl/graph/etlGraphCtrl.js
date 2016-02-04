;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlGraphController', ['$scope', '$state', '$etlGraphHTTP', etlGraphController]);

  function etlGraphController($scope, $state, $etlGraphHTTP) {
    var data = JSON.parse($state.params.data),
        columns = JSON.parse(data.colsInfo.cols);
    $scope.columns = columns;
    function renderGraph() {
      var margin = {top: 20, right: 20, bottom: 30, left: 50},
          width = 960 - margin.left - margin.right,
          height = 500 - margin.top - margin.bottom;

      var formatDate = d3.time.format("%d-%b-%y");

      var x = d3.time.scale()
          .range([0, width]);

      var y = d3.scale.linear()
          .range([height, 0]);

      var xAxis = d3.svg.axis()
          .scale(x)
          .orient("bottom");

      var yAxis = d3.svg.axis()
          .scale(y)
          .orient("left");
      
      // Create the Range object
      var rangeObj = new Range();

      // Select all of theParent's children
      rangeObj.selectNodeContents(document.getElementById('area57'));

      // Delete everything that is selected
      rangeObj.deleteContents();

      var line = d3.svg.line()
          .x(function(d) { return x(d.date); })
          .y(function(d) { return y(d.close); });

      var svg = d3.select("#area57").append("svg")
          .attr("width", width + margin.left + margin.right)
          .attr("height", height + margin.top + margin.bottom)
        .append("g")
          .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      d3.tsv("/assets/data.tsv", type, function(error, data) {
        if (error) throw error;

        x.domain(d3.extent(data, function(d) { return d.date; }));
        y.domain(d3.extent(data, function(d) { return d.close; }));

        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis);

        svg.append("g")
            .attr("class", "y axis")
            .call(yAxis)
          .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", 6)
            .attr("dy", ".71em")
            .style("text-anchor", "end")
            .text("Price ($)");

        svg.append("path")
            .datum(data)
            .attr("class", "line")
            .attr("d", line);
      });

      function type(d) {
        d.date = formatDate.parse(d.date);
        d.close = +d.close;
        return d;
      }
    }

    function successRead(response) {
      // console.log(response);
    }

    function errorRead(reason) {
      console.log('reason', reason);
    }

    $etlGraphHTTP
      .requestContent(data)
      .then(successRead, errorRead);

    $scope.doRender = function doRender() {
      renderGraph();
    };

    $scope.selectedRow = undefined;
    $scope.selectedColumn = undefined;
  }
})();
