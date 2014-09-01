var elasticsearch = require('elasticsearch'),
	EventEmitter = require('events').EventEmitter,
	util = require('util');
	
	


function Armstrong( config ){
	EventEmitter.call(this);
	var self = this;
	
	this.client = new elasticsearch.Client({
		host: config.host, //'localhost:9200',
		log: config.log //'trace'
	});
	
	this.config = { type:'article' };
	for ( var prop in config ) this.config[prop] = config[prop];
	
	this.client.ping({
		// ping usually has a 100ms timeout
		requestTimeout: 1000,
		// undocumented params are appended to the query string
		hello: "elasticsearch!"
	}, function ( err ) {
		if ( err ) self.emit('error',err);
	});
	return this;
}
util.inherits( Armstrong, EventEmitter );


Armstrong.prototype.map = function( callback ){
	this.client.indices.putMapping({
		index : this.config.index,
		type : this.config.type,
		body : {
			properties : {
				published : { type : "date" },
				updated : { type : "date" },
				views : { type : "integer" },
				body : { type : "string", analyzer : "english" },
				body_plain : { type : "string", analyzer : "english" }
			}
		}
/*		"tweet" : {
	        "properties" : {
	            "message" : {"type" : "string", "store" : true }
	        }
	    }*/
	}, callback );
};


Armstrong.prototype.suggest = function( term, callback ){
	this.client.suggest({
		index: this.config.index,
		type: this.config.type,
		body: {
			suggest: {
				text : term,
				term : {
					field: 'body',
				}
			}
		}
	},callback);
};

Armstrong.prototype.search = function( query, callback ){
	this._search({
		query: {
			match: {
				body_plain: query
			}
		},
		highlight : {
			pre_tags : ["<em>"],
			post_tags : ["</em>"],
			fields : {
				body_plain : {}
			}
		}
	}, callback );
};

Armstrong.prototype._search = function( query, callback ){
	
	var body = query;
	var filter = {
		and : [
			{ term : { status : 'published' } },
			{ missing : { field : 'alternate_titles' } }
		]
	};
	
	console.log("Seaching - missing titles");
	if ( typeof body == "string" ) {
		console.log("is string");
		body = {
			query: { 
				filtered : {
					query : query,
					filter : filter,
				}
			}
		};
	} else {
		console.log("is obj");
		body.filter = filter;
	}
	
	this.client.search({
		index : this.config.index,
		type : this.config.type,
		body : body
	}).then(function (res) {
		callback( undefined, res, res.hits.hits );
	}, function ( err, res ) {
		callback( err, res );
	});
	
};

Armstrong.prototype.recent = function( conf, callback ){
	
	this._search({
		size : conf.count || 10,
		sort : [ { published : "desc" } ], // need to figure out a clean way to push mappings to ES
		query: { 
			filtered : {
				query : { match_all:{} },
				filter : {
					and : [
						{ term : { status : 'published' } },
						{ missing : { field : 'alternate_titles' } }
					]
				}
			}
		}
	}, callback );
};

Armstrong.prototype.popular = function( conf, callback ){
	this._search({
		size : conf.count || 10,
		sort : [ { views : "desc" } ], // need to figure out a clean way to push mappings to ES
		query: { 
			filtered : {
				query : { match_all:{} },
				filter : {
					and : [
						{ term : { status : 'published' } },
						{ missing : { field : 'alternate_titles' } }
					]
				}
			}
		}
	}, callback );
};

Armstrong.prototype.getDocByUrl = function( url, callback ){
	var self = this;
	this.getDocByField( 'url', url, function( err, res ){
		if ( err ) return callback(err);
		if ( !res ) return callback();
		
		var views = res._source.views || 0;
		self.incrementViewCounter( res._id, ++views );
		callback ( undefined, res );
	});
};

Armstrong.prototype.incrementViewCounter = function( url, views, callback ){
	var self = this;
	this.client.update({
		index : this.config.index,
		type : this.config.type,
		id : url,
		body: {
			//script: 'ctx._source.views += views',
			doc: { views : views }
		}
	}, function ( err, res ) {
		if ( callback ) return callback( err, res );
		if ( err ) console.error('increment',self.config.index,self.config.type,url,views,err);
	})
};


Armstrong.prototype.getDocsByField = function( field, value, callback ){
	
	var match = {};
	match[field] = value;
	
	this.client.search({
		index : this.config.index,
		type : this.config.type,
		body : {
		//	query: {
		//		match: match
		//	}
			query: { 
				filtered : {
					query : { match:match },
					filter : {
						and : [
							{ term : { status : 'published' } },
							{ missing : { field : 'alternate_titles' } }
						]
					}
				}
			}
		}
	}).then(function (resp) {
		var hits = resp.hits.hits;
		
		callback( undefined, hits );
	}, function (err) {
		callback(err);
	});
	
};

Armstrong.prototype.getDocByField = function( field, value, callback ){
	this.getDocsByField( field, value, function( err, hits ){
		if ( err ) return callback(err);
		// hack filter to make sure the hit is an exact match on the field
		var correctHits = hits.filter(function(it){ return it._source && it._source[field] == value });
		callback( undefined, correctHits[0] );
	});
};

Armstrong.prototype.save = function( doc, callback ){
	var post = {
		index : this.config.index,
		type : this.config.type,
		body : doc
	};
	
	this.client.index( post, function ( err, res ) {
		if ( callback ) callback( err, res );
	});
};

Armstrong.prototype.similar = function( id, callback ){
	this.client.mlt({
		index : this.config.index,
		type : this.config.type,
		id : id,
		mlt_fields : 'body',
		searchSize : 5,
		min_term_freq : 3,
		min_doc_freq : 1
	}, function ( err, res ) {
		callback( err, res );
	});
};

Armstrong.prototype.index = function( doc, id, callback ){
	// make id optional
	if ( !callback && id instanceof Function ){ callback = id; id = undefined; }
	
	var post = {
		index : this.config.index,
		type : this.config.type,
		consistency : "quorum",
		body : doc,
		indexed : new Date()
	};
	if ( id ) post.id = id;
	
	this.client.index( post, callback );
};

Armstrong.prototype.update = function( doc, id, callback ){
	// make id optional
	if ( !callback && id instanceof Function ){ callback = id; id = undefined; }
	
	doc.updated = new Date();
	
	var post = {
		index : this.config.index,
		type : this.config.type,
		id : id,
		body : {
			doc : doc,
		}
	};
	
	this.client.update( post, callback );
};


Armstrong.prototype.upsert = function( doc, id, callback ){
	var self = this;
	this.update( doc, id, function( err, res ){
		if ( err && err.message.indexOf("DocumentMissingException") > -1 ) {
			doc.views = 0;
			return self.index(doc,id,callback); 
		}
		//console.log(doc,id,callback)
		callback( err, res );
		//if ( err ) return this.insert( doc, id, callback );
	});
};






exports.new = function( config ){
	return new Armstrong( config );
};
