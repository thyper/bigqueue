
exports.redisListToObj = function(list){
    var o = {}
    for(var i = 0; i< list.length; i=i+2){
        o[list[i]] = list[i+1]
    }
    return o;
}

