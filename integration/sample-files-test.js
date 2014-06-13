
var armstrong = require('../index').new({
		index:'armstrong-test',
		host:'localhost:9200',
		log:'warning'
	}),
	rootPath = "./resources",
	assert = require("assert");

function readFiles( dir, callback ){
	var fs = require('fs');
	fs.readdir( dir, function( err, files ){
		if ( err ) return callback(err);
		files.forEach(function(file){
			fs.readFile( dir+file, 'utf8', function( err, content ){
				callback( err, file, content );
			});
		});
	});
}

	
describe('MD samples', function(){

	it("should be indexed without error", function(done){
		
		var count = 0;
		
		readFiles( process.cwd()+"/integration/resources/", function ( err, file, content ) {
			if ( err ) return done(err);
			
			count++;
			
			armstrong.index({
				url : '/some-test-data/'+file,
				body : content
			}, file, function( err, res ){
				
				assert.equal( res.ok, true );
				
				armstrong.getDocByUrl( '/some-test-data/'+file, function( err, hit ){
					assert.equal( hit._source.url, '/some-test-data/'+file );
					if ( --count === 0 ) done();
				});
			});
		});
	});
});


