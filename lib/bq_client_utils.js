
exports.redisListToObj = function(list){
    var o = {}
    for(var i = 0; i< list.length; i=i+2){
        o[list[i]] = list[i+1]
    }
    return o;
}

/**
  * Delete recursively all 
  */ 
exports.deleteZkRecursive = function(zk,path,cb,c){
        var self = this
        var c = c || 0
        zk.a_get_children(path,false,function(rc,error,children){
            var total = 0
            if(children)
                total = children.length
            var count = 0
            var deleteThis = function(){
                 zk.a_delete_(path,-1,function(rc,error){
                 })
            }
            if(total == 0){
                    deleteThis()
                    cb()
            }            
            for(var i in children){
                var p = path+"/"+children[i]
                var nc = c++
                self.deleteZkRecursive(zk,p,function(){
                    count++;
                    if(count >= total){
                        zk.a_delete_(p,-1,function(rc,error){
                            deleteThis()
                            cb()
                        })
                     }
                },nc)
            }
        })

    }

exports.getDataFromChilds=function(zkClient,path,callback){
    var self = this
    zkClient.a_exists(path,false,function(rc,error){
        if(rc!=0){
            callback(error)
            return
        }
        zkClient.a_get_children(path, false, function(rc,error,childrens){
            if(rc != 0){
                callback(error)
                return;
            }
            if(childrens.length == 0){
                callback(undefined,[])
                return
            }
            var exec = 0
            var datas = []
            var errors = []
            childrens.forEach(function(val){
                zkClient.a_get(path+"/"+val,false,function(rc,error,stat,data){
                    if(rc != 0){
                        errors.push(error)
                    }else{
                        try{
                            datas.push(JSON.parse(data))
                        }catch(e){
                            datas.push(data.toString)
                        }
                        exec++
                        if(exec == childrens.length){
                            if(errors.length>0){
                                callback(errors)
                            }else{
                                callback(undefined,datas)
                            }
                        }
                    }
                })
            })
            
        })
    })
}

exports.checkProperty = function(property){
    if(property == undefined || property == null)
       return false
    if(typeof(property) == "string" && property == "")
        return false
    return true
}

exports.checkNodeData = function(nodeData){
    if(nodeData == undefined || nodeData == null)
        return false;
    if(!this.checkProperty(nodeData.host) || 
            !this.checkProperty(nodeData.port) ||
            !this.checkProperty(nodeData.status) 
       ){
        return false
    }
    return true;
}

exports.checkJournalData = function(nodeData){
    if(nodeData == undefined || nodeData == null)
        return false;
    if(!this.checkProperty(nodeData.host) || 
            !this.checkProperty(nodeData.port) ||
            !this.checkProperty(nodeData.status)){
        return false
    }
    return true;

}


