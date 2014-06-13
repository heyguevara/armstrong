
function Article(){
	
	this.tags = [];

}
exports.new = function(conf){
	return new Article(conf);
}