var app = angular.module('searchApp', ['ngSanitize']);

app.controller('SearchCtrl', function ($scope, $sce, $http, $templateCache) {
	$scope.results = [];

	$scope.search = function() {

		var root_url = "http://localhost:8983/solr";
		//root_url = "http://localhost:8764/api/apollo/solr";
		var index = "test2";
		var term = '';
		
		// not always true. ex: "string1 string2"~30
		//var lastChar = $scope.query.substr($scope.query.length - 1);
		//var firstChar = $scope.query.substr(0, 1);

		//if(lastChar == '"' && firstChar == '"'){
		if($scope.query.indexOf('"') > -1) {
			term = "text:"+$scope.query+"\nresource_path:"+$scope.query;
		}
		else {
			var text_terms = [];
			var resource_path_terms = [];
			var term_array = $scope.query.split(' ');
			angular.forEach(term_array, function(item, index){
				text_terms.push('text:"'+item+'"');
				resource_path_terms.push('resource_path:"'+item+'"');
			});
			term = text_terms.join(" OR ");
			term += "\n";
			term += resource_path_terms.join(" OR ");
		}


		var url = root_url+"/"+index+"/select?"+
			"q="+encodeURIComponent(term)+"&"+
			"fl="+encodeURIComponent("content,id")+"&"+
			"wt=json&"+
			"json.wrf=JSON_CALLBACK&"+
			"hl=true&"+
			"hl.fragsize=500&"+
			"hl.fl="+encodeURIComponent("content,resource_path")+"&"+
			"hl.simple.pre="+encodeURIComponent("<span class='highlight'>")+"&"+
			"hl.simple.post="+encodeURIComponent("</span>");

		$http({method: "JSONP", url: url, cache: $templateCache}).
			success(function(data, status, header, config){

				//console.log(data);

				var results = [];
				angular.forEach(data.response.docs, function(doc, kdoc){
					var item = { 'id' : doc.id, 'text' : '', 'path' : "file://"+doc.id.replace("\\", "/") };

					var highlighting_result = data.highlighting[doc.id];
					if(highlighting_result != undefined){
						var highlighting_content = highlighting_result.content;
						if(highlighting_content != undefined){
							item.text = $sce.trustAsHtml("..."+highlighting_content[0]+"...");
						}

						var highlighting_resource_path = highlighting_result.resource_path;
						if(highlighting_resource_path != undefined){
							item.id = $sce.trustAsHtml(highlighting_resource_path[0]);
						}
					}

					results.push(item)
				});

				$scope.results = results;
			}).
			error(function(data, status, header, config){

			});


		/*results = [
			{
				'id': 'C:\\Users\\Mathias\\Documents\\aranger\\2.3.1_Plan_Assurance_Qualité_Maintenance_GA8000-QPR0004-A.pdf',
		 		'text': $sce.trustAsHtml('RATP - RTHD GA8000-QPR0004-A02.doc Page 1/ 17 Tous droits réservés. <span class="highlight">Diffusion</span> et copie de ce document, utilisation et communication de son contenu sont interdits sans autorisation écrite. RATP - RTHD RENOUVELLEMENT DES INFRASTRUCTURES')
		 	}
		];
		$scope.results = results;*/
		
	}
});