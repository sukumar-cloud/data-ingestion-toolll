var app = angular.module('IngestionApp', []);

app.directive('fileModel', ['$parse', function ($parse) {
    return {
        restrict: 'A',
        link: function(scope, element, attrs) {
            var model = $parse(attrs.fileModel);
            var modelSetter = model.assign;
            element.bind('change', function(){
                scope.$apply(function(){
                    modelSetter(scope, element[0].files[0]);
                });
            });
        }
    };
}]);

app.controller('MainController', ['$http', '$scope', function($http, $scope) {
    // Set the base URL for API calls - uses environment variable or falls back to localhost
    var API_BASE_URL = window.API_BASE_URL || 'http://localhost:8080/api';
    console.log('Using API base URL:', API_BASE_URL);
    var vm = this;

    // State
    vm.sourceType = 'clickhouse';
    vm.targetType = 'clickhouse';
    vm.source = {};
    vm.target = {};
    vm.columns = [];
    vm.sourceTables = [];
    vm.previewRows = [];
    vm.previewColumns = [];
    vm.status = '';
    vm.error = '';
    vm.resultCount = null;

    // Source connect
    vm.connectSource = function() {
        vm.status = 'Connecting to source...';
        vm.error = '';
        $http.post(API_BASE_URL + '/source/connect', vm.source)
            .then(function(res) {
                vm.status = res.data;
                if(vm.sourceType === 'clickhouse') {
                    vm.listSourceTables();
                }
            }, function(err) {
                vm.error = err.data || 'Connection failed';
            });
    };

    vm.listSourceTables = function() {
        $http.post(API_BASE_URL + '/source/tables', vm.source)
            .then(function(res) {
                vm.sourceTables = res.data.tables || [];
            }, function(err) {
                vm.error = err.data || 'Failed to fetch tables';
            });
    };

    vm.loadSourceColumns = function() {
        var payload = angular.copy(vm.source);
        payload.tableName = vm.source.tableName;
        $http.post(API_BASE_URL + '/source/columns', payload)
            .then(function(res) {
                vm.columns = (res.data.columns || []).map(function(col) {
                    return { name: col, selected: true };
                });
            }, function(err) {
                vm.error = err.data || 'Failed to fetch columns';
            });
    };

    // Target connect
    function trimStr(v) { return (typeof v === 'string') ? v.trim() : v; }

    function sanitizeTarget(target, targetType) {
        // Fix typo in user field
        if (target.user && target.user.toLowerCase() === 'defualt') {
            target.user = 'default';
        }
        return {
            type: targetType || '',
            host: trimStr(target.host) || '',
            port: target.port || '',
            database: trimStr(target.database) || '',
            user: trimStr(target.user) || '',
            password: trimStr(target.password) || '',
            jwtToken: target.jwtToken || '',
            filePath: target.filePath || '',
            delimiter: target.delimiter || '',
            tableName: trimStr(target.tableName) || '',
            useHttps: !!target.useHttps
        };
    }

    vm.connectTarget = function() {
        vm.status = 'Connecting to target...';
        vm.error = '';
        var payload = sanitizeTarget(vm.target, vm.targetType);
        console.log('Target connect payload:', payload);
        $http.post(API_BASE_URL + '/target/connect', payload)
            .then(function(res) {
                vm.status = res.data;
            }, function(err) {
                if (err.data && err.data.message) {
                    vm.error = 'Backend error: ' + err.data.message;
                } else if (err.data) {
                    vm.error = 'Backend error: ' + JSON.stringify(err.data);
                } else {
                    vm.error = 'Connection failed (400)';
                }
            });
    };



    // Preview CSV
    vm.previewCsv = function(side) {
        var file = (side === 'source') ? vm.sourceFile : vm.targetFile;
        var delimiter = (side === 'source') ? vm.source.delimiter : vm.target.delimiter;
        var fd = new FormData();
        fd.append('file', file);
        fd.append('delimiter', delimiter || ',');
        $http.post(API_BASE_URL + '/preview-csv', fd, {
            transformRequest: angular.identity,
            headers: { 'Content-Type': undefined }
        }).then(function(res) {
            vm.previewRows = res.data.data || [];
            vm.previewColumns = res.data.columns || [];
        }, function(err) {
            vm.error = err.data || 'Failed to preview CSV';
        });
    };

    // Preview Data (ClickHouse or File)
    vm.previewData = function() {
        if(vm.sourceType === 'clickhouse') {
            var payload = angular.copy(vm.source);
            payload.tableName = vm.source.tableName;
            $http.post(API_BASE_URL + '/source/preview', payload)
                .then(function(res) {
                    vm.previewRows = res.data.data || [];
                    vm.previewColumns = res.data.columns || [];
                }, function(err) {
                    vm.error = err.data || 'Failed to preview ClickHouse data';
                });
        } else {
            vm.previewCsv('source');
        }
    };

    // Start Ingestion
    vm.startIngestion = function() {
        vm.status = 'Ingesting...';
        vm.error = '';
        vm.resultCount = null;
        var selectedColumns = vm.columns.filter(function(col) { return col.selected; }).map(function(col) { return col.name; });
        var ingestionRequest = {
            source: angular.copy(vm.source),
            target: sanitizeTarget(vm.target, vm.targetType),
            columns: selectedColumns
        };
        if (vm.sourceType === 'file' && vm.targetType === 'clickhouse') {
            // File -> ClickHouse upload
            var fd = new FormData();
            fd.append('file', vm.sourceFile);
            fd.append('ingestionRequest', new Blob([JSON.stringify(ingestionRequest)], { type: 'application/json' }));
            $http.post(API_BASE_URL + '/transfer-csv-to-clickhouse', fd, {
                transformRequest: angular.identity,
                headers: { 'Content-Type': undefined }
            }).then(function (res) {
                vm.status = res.data.status;
                vm.resultCount = res.data.recordsTransferred;
            }, function (err) {
                vm.error = err.data || 'Ingestion failed';
            });
        } else if (vm.sourceType === 'file' && vm.targetType === 'file') {
            // Not implemented in backend – avoid sending wrong request
            vm.status = '';
            vm.error = 'File -> File ingestion is not supported yet. Please choose ClickHouse as target.';
            return;
        } else {
            // General ingestion (ClickHouse to File or ClickHouse to ClickHouse)
            $http.post(API_BASE_URL + '/ingest', ingestionRequest)
                .then(function(res) {
                    vm.status = res.data.status;
                    vm.resultCount = res.data.recordsTransferred;
                }, function(err) {
                    vm.error = err.data || 'Ingestion failed';
                });
        }
    };

    // Target type change
    vm.onTargetTypeChange = function() {
        vm.target = {};
    };
    vm.onSourceTypeChange = function() {
        vm.source = {};
        vm.columns = [];
        vm.previewRows = [];
        vm.previewColumns = [];
    };
}]);
