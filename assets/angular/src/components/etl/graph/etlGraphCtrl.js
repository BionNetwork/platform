;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlGraphController', ['$scope', '$state', '$etlGraphHTTP', etlGraphController]);

  function etlGraphController($scope, $state, $etlGraphHTTP) {
    var columns_ = JSON.parse($state.params.data),
        columns = JSON.parse(columns_.colsInfo.cols),
        graph = [],
        data_;
    
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
          .x(function(d) { return x(d[$scope.selectedRow]); })
          .y(function(d) { return y(d[$scope.selectedColumn]); });

      var svg = d3.select("#area57").append("svg")
          .attr("width", width + margin.left + margin.right)
          .attr("height", height + margin.top + margin.bottom)
        .append("g")
          .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      //d3.tsv("/assets/angular/dist/data.tsv", type, function(error, data) {
      //  if (error) throw error;

        x.domain(d3.extent(data_, function(d) { return d[$scope.selectedRow]; }));
        y.domain(d3.extent(data_, function(d) { return d[$scope.selectedColumn]; }));

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
            .datum(data_)
            .attr("class", "line")
            .attr("d", line);
      //});

      //function type(d) {
      //  d.date = formatDate.parse(d.date);
      //  d.close = +d.close;
      //  return d;
      //}
    }

    function successRead(response) {
      data_ = response.data.data;
    }

    function errorRead(reason) {
      console.log('reason', reason);
    }

    $etlGraphHTTP
      .requestContent(columns_)
      .then(successRead, errorRead);

    $scope.doRender = function doRender() {
      renderGraph();
    };

    $scope.selectedRow = undefined;
    $scope.selectedColumn = undefined;
  }
})();
