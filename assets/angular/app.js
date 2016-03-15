
(function main() {
  'use strict';
  angular.module('BIPlatform', [
    'ui.router'
  ]);
})();



;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .config([
      '$stateProvider',
      '$urlRouterProvider',
      router
    ]);

  function router($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('not-found', {
        url: "/not-found",
        template: "not found"
      })
      .state('home', {
        url: "/",
        templateUrl: '/assets/angular/main/mainTmpl.html',
        controller: "mainCtrl"
      });

    $urlRouterProvider.otherwise(function($injector, $location) {
      if ($location.path() === '') {
        return '/';
      }
      //return '/not-found';
    });
  }
})();

;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('mainCtrl', ['$scope', mainCtrl]);

  function mainCtrl($scope) {
  }
})();

;(function() {
  'use strict';

  angular
    .module('BIPlatform')
    .controller('logoController', ['$scope', logoController]);

  function logoController($scope) {
    $scope.homeRef = $scope.homeRef || 'home';
  }

})();


(function sharedLogo() {
  'use strict';
  angular
    .module('BIPlatform')
    .directive('logo', logo);

  function logo() {
    return {
      restrict: 'E',
      scope: {
        homeRef: '='
      },
      controller: 'logoController',
      templateUrl: '/assets/angular/shared/logo/logoTmpl.html'
    };
  }
})();

;(function() {
  'use strict';

  angular
    .module('BIPlatform')
    .controller('menuController', ['$scope', menuController]);

  function menuController($scope) {
  }

})();


(function sharedLogo() {
  'use strict';
  angular
    .module('BIPlatform')
    .directive('menu', menu);

  function menu() {
    return {
      restrict: 'E',
      scope: {
      },
      controller: 'menuController',
      templateUrl: '/assets/angular/shared/menu/menuTmpl.html'
    };
  }
})();

;(function() {
  "use strict";

  angular
    .module('BIPlatform')
    .config([
      '$stateProvider',
      '$urlRouterProvider',
      route
    ]);

  function route($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('etl.add', {
        url: "/add",
        controller: 'etlFormController',
        templateUrl: '/assets/angular/components/etl/form/etlFormTmpl.html'
      })
      .state('etl.edit', {
        url: "/edit/:id",
        controller: 'etlFormController',
        templateUrl: '/assets/angular/components/etl/form/etlFormTmpl.html'
      });

  }

})();


;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlFormController', ['$scope', '$state', '$etlHTTP', etlFormController]);

  function etlFormController($scope, $state, $etlHTTP) {
    $scope.etl = {
    };

    function successRead(etl) {
      $scope.etl = etl;
    }

    function successCreate(etl) {
      $state.go('etl.view');
    }

    function successUpdate(etl) {
      $state.go('etl.view');
    }

    function errorHandler(reason) {
      console.log('error', reason);
    }

    if ($state.params.id) {
      $etlHTTP.read({
        id: $state.params.id
      }).then(successRead, errorHandler);
    }

    $scope.submit = function submit(etl) {
      if (etl.id) {
        $etlHTTP
          .update(etl)
          .then(successUpdate, errorHandler);
      }
      else {
        $etlHTTP
          .add(etl)
          .then(successCreate, errorHandler);
      }
    };
  }
})();

;(function() {
  "use strict";

  angular
    .module('BIPlatform')
    .config([
      '$stateProvider',
      '$urlRouterProvider',
      route
    ]);

  function route($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('etl.graph', {
        url: "/graph/:data",
        controller: 'etlGraphController',
        templateUrl: '/assets/angular/components/etl/graph/etlGraphTmpl.html'
      });

  }

})();


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

;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .service('$etlGraphHTTP', ['$http', '$q', etlGraphHTTP]);

  function etlGraphHTTP($http, $q) {

    this.requestContent = function(request) {
      var request_ = JSON.parse(JSON.stringify(request.colsInfo));
      return $http.post(request.url, request_, {
        transformRequest: function(obj) {
          var str = [];
          for(var p in obj) {
            str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));
          }
          return str.join("&");
        },
        headers: {
          'X-CSRFToken': csrftoken,
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      });
    }
  }

})();

;(function() {
  "use strict";

  angular
    .module('BIPlatform')
    .config([
      '$stateProvider',
      '$urlRouterProvider',
      route
    ]);

  function route($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('etl.manage', {
        url: "/manage/:id",
        controller: 'etlManageController',
        templateUrl: '/assets/angular/components/etl/manage/etlManageTmpl.html'
      });

  }

})();


;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlManageController', ['$scope', '$state', '$etlManageHTTP', etlManageController]);

  function etlManageController($scope, $state, $etlManageHTTP) {
    var id = $state.params.id;

    $scope.getRequest_RefreshData = function() {
      $state.go('etl.graph', { data: JSON.stringify(window.refreshData_request) });
    };
    getConnectionData('/etl/datasources/get_data/' + id + '/', '/etl/datasources/remove_all_tables/');
  }
})();

;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .service('$etlManageHTTP', ['$http', '$q', etlManageHTTP]);

  function etlManageHTTP($http, $q) {

  }

})();

;(function() {
  "use strict";

  angular
    .module('BIPlatform')
    .config([
      '$stateProvider',
      '$urlRouterProvider',
      route
    ]);

  function route($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('etl.view', {
        url: "/view",
        controller: 'etlViewController',
        templateUrl: '/assets/angular/components/etl/view/etlViewTmpl.html'
      });
  }
})();


;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlViewController', ['$scope', '$etlHTTP', etlViewController]);

  function etlViewController($scope, $etlHTTP) {
    $scope.etls = [];
    $scope.currentEtl = undefined;

    function successRead(etls) {
      $scope.etls = etls;
    }

    function successRemove(etl) {
      var etls = $scope.etls,
          l = etls.length,
          found = false,
          i;

      for (i = 0; i < l; i++) {
        if (etls[i].id == etl.id) {
          found = true;
          etls.splice(i, 1);
          break;
        }
      }

      if (found) {
        $('#etlRemoveModal').modal('hide');
      }
      else {
        console.log('Something went wrong...');
      }
    }

    function errorHandler(reason) {
      console.log('error', reason);
    }

    $etlHTTP
      .read()
      .then(successRead, errorHandler);

    $scope.confirmRemove = function confirmRemove() {
      $etlHTTP
        .remove($scope.currentEtl)
        .then(successRemove, errorHandler);
    };

    $scope.cancelRemove = function cancelRemove() {
      console.log('cancelRemove item', $scope.currentEtl);
    };

    $scope.prepareRemove = function prepareRemove(item) {
      $scope.currentEtl = item;
    };
  }
})();

;(function() {
  "use strict";

  angular
    .module('BIPlatform')
    .config([
      '$stateProvider',
      '$urlRouterProvider',
      route
    ]);

  function route($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('users.add', {
        url: "/add",
        controller: 'usersFormController',
        templateUrl: '/assets/angular/components/users/form/usersFormTmpl.html'
      })
      .state('users.edit', {
        url: "/edit/:id",
        controller: 'usersFormController',
        templateUrl: '/assets/angular/components/users/form/usersFormTmpl.html'
      });

  }

})();


;(function() {
	'use strict';
	angular
		.module('BIPlatform')
		.controller('usersFormController', ['$scope', '$state', '$usersHTTP', usersFormController]);

	function usersFormController($scope, $state, $usersHTTP) {
		$scope.user = {
			status: 'active'
		};

		function successRead(user) {
			$scope.user = user;
		}

		function successCreate(user) {
			$state.go('users.view');
		}

		function successUpdate(user) {
			$state.go('users.view');
		}

		function errorHandler(reason) {
			console.log('error', reason);
		}

		if ($state.params.id) {
			$usersHTTP.read({
				id: $state.params.id
			}).then(successRead, errorHandler);
		}

		$scope.submit = function submit(user) {
			if (user.id) {
				$usersHTTP
					.update(user)
					.then(successUpdate, errorHandler);
			}
			else {
				$usersHTTP
					.add(user)
					.then(successCreate, errorHandler);
			}
		};
	}
})();

;(function() {
  "use strict";

  angular
    .module('BIPlatform')
    .config([
      '$stateProvider',
      '$urlRouterProvider',
      route
    ]);

  function route($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('users.view', {
        url: "/view",
        controller: 'usersViewController',
        templateUrl: '/assets/angular/components/users/view/usersViewTmpl.html'
      });
  }
})();


;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('usersViewController', ['$scope', '$usersHTTP', usersViewController]);

  function usersViewController($scope, $usersHTTP) {
    $scope.users = [];
    $scope.currentUser = undefined;

    function successRead(users) {
      $scope.users = users;
    }

    function successRemove(user) {
      var users = $scope.users,
          l = users.length,
          found = false,
          i;

      for (i = 0; i < l; i++) {
        if (users[i].id == user.id) {
          found = true;
          users.splice(i, 1);
          break;
        }
      }

      if (found) {
        $('#userRemoveModal').modal('hide');
      }
      else {
        console.log('Something went wrong...');
      }
    }

    function errorHandler(reason) {
      console.log('error', reason);
    }

    $usersHTTP
      .read()
      .then(successRead, errorHandler);

    $scope.confirmRemove = function confirmRemove() {
      $usersHTTP
        .remove($scope.currentUser)
        .then(successRemove, errorHandler);
    };

    $scope.cancelRemove = function cancelRemove() {
      console.log('cancelRemove item', $scope.currentUser);
    };

    $scope.prepareRemove = function prepareRemove(item) {
      $scope.currentUser = item;
    };
  }
})();

;(function() {
  "use strict";

  angular
    .module('BIPlatform')
    .config([
      '$stateProvider',
      '$urlRouterProvider',
      route
    ]);

  function route($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('etl', {
        abstract: true,
        url: "/etl",
        controller: 'etlController',
        templateUrl: '/assets/angular/components/etl/etlTmpl.html'
      });
  }

})();


;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('etlController', ['$scope', etlController]);

  function etlController($scope) {

  }
})();

;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .service('$etlHTTP', ['$http', '$q', etlHTTP]);

  function etlHTTP($http, $q) {
    var etls = [];

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
        // deferred.resolve(JSON.parse(JSON.stringify(etls)));
        return $http.get('/etl/api/datasources/')
          .then(function(response) {
            etls = response.data.data;
            return etls;
          });
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

;(function() {
  "use strict";

  angular
    .module('BIPlatform')
    .config([
      '$stateProvider',
      '$urlRouterProvider',
      route
    ]);

  function route($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('users', {
        abstract: true,
        url: "/users",
        controller: 'usersController',
        templateUrl: '/assets/angular/components/users/usersTmpl.html'
      });
  }

})();


;(function() {
  'use strict';
  angular
    .module('BIPlatform')
    .controller('usersController', ['$scope', usersController]);

  function usersController($scope) {

  }
})();

;(function() {
	'use strict';
	angular
		.module('BIPlatform')
		.service('$usersHTTP', ['$http', '$q', usersHTTP]);

	var url_read = '/api/v1/users/';
	var url_update = url_read;
	var url_create = url_read;
	var url_delete = url_read;

	function usersHTTP($http, $q) {
		var users = [];

		this.add = function add(user) {
			var url = url_read;
			var config = {
				url: url,
				data: user,
				headers: {
					'X-CSRFToken': csrftoken
				},
				method: 'POST'
			};
			return $http(config).then(function(response) {
				return response.data;
			});
		};

		this.update = function update(user) {
			var deferred = $q.defer();
			var url = url_update;

			if (user) {
				if (!user.id) {
					deferred.reject({
						message: 'incorrect user - has not id'
					});
					return deferred.promise;
				}
				url = url + user.id;
			}
			else {
				deferred.reject({
					message: 'no user to update was provided'
				});
				return deferred.promise;
			}
			var config = {
				url: url,
				data: user,
				headers: {
					'X-CSRFToken': csrftoken
				},
				method: 'PATCH'
			};
			return $http(config).then(function(response) {
				return response.data;
			});
		};

		this.read = function read(criteria) {
			var url = url_read;
			if (criteria) {
				if (criteria.id) {
					url = url + criteria.id;
				}
			}
			var config = {
				url: url,
				method: 'GET'
			};
			return $http(config).then(function(response) {
				return response.data;
			});
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
