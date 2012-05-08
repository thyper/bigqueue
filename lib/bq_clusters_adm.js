var events = require("events"),
    bqc = require("../lib/bq_cluster_client.js"),
    ZK = require("zookeeper"),
    utils = require("../lib/bq_client_utils.js"),
    log = require("node-logging")



function BigQueueClustersAdmin(zkClient,zkConfig,defaultCluster,zkBqPath,zkFlags,createNodeClientFunction,createJournalClientFunction){
    this.clusterClients = []
    this.zkClient = zkClient
    this.zkConfig = zkConfig
    this.defaultCluster = defaultCluster
    this.zkFlags = zkFlags || 0
    this.zkBqPath = zkBqPath
    this.zkTopicsIndexPath = zkBqPath+"/admin/indexes/topics"
    this.zkGroupsIndexPath = zkBqPath+"/admin/indexes/groups"
    this.zkClustersPath = zkBqPath+"/clusters"
    this.createNodeClientFunction = createNodeClientFunction
    this.createJournalClientFunction = createJournalClientFunction
    this.clusterFolders = ["nodes","journals","topics","entrypoints"]
       
    this.init()

}

BigQueueClustersAdmin.prototype = new events.EventEmitter()

BigQueueClustersAdmin.prototype.shutdown = function(){
    if(this.zkClient)
        this.zkClient.close()
    var clusterNames = Object.keys(this.clusterClients)
    for(var c in clusterNames){
        this.clusterClients[clusterNames[c]].shutdown()
    }
}

BigQueueClustersAdmin.prototype.init = function(){
    var self = this
    this.zkClient.connect(function(err){
        if(err){
            log.err("Error connecting to zookeeper ["+err+"]",true)
            self.emit("error",err)
            return
        }
        self.emit("ready")
    })

}

BigQueueClustersAdmin.prototype.createBigQueueCluster = function(clusterData,callback){
    var self = this
    if(!clusterData.name){
        callback("No cluster name exists")
        return
    }
    var clusterPath = this.zkClustersPath+"/"+clusterData.name
    this.zkClient.a_create(clusterPath,"",this.zkFlags,function(rc,error,stat){
        if(rc!=0){
            callback(error)
            return
        }
        var exec = 0
        var errors = []
         for(var i in self.clusterFolders){
            self.zkClient.a_create(clusterPath+"/"+self.clusterFolders[i],"",self.zkFlags,function(rc,error,stat){
                exec++
                if(rc!=0){
                    errors.push(error)
                }
                if(exec == self.clusterFolders.length){
                    if(errors.length > 0){
                        callback("Error creatng clusters ["+JSON.stringify(errors)+"]")
                    }else{
                        self.createNode(clusterData.name,clusterData.journals,"journals",function(err){
                            if(err){
                                callback(err)
                                return
                            }
                            self.createNode(clusterData.name,clusterData.nodes,"nodes",function(err){
                                if(err){
                                    callback(err)
                                    return
                                }
                                self.createNode(clusterData.name,clusterData.entrypoints,"entrypoints",function(err){
                                    if(err){
                                        callback(err)
                                        return
                                    } 
                                    callback()
                                })
                            })
                        })
                    }
                }
            })
        }
    })
}

BigQueueClustersAdmin.prototype.validateNodeJson = function(node,callback){
    if(!node.name || !node.config){
        callback("Node should have a name and a config")
        return false
    }
    return true
}

BigQueueClustersAdmin.prototype.addNodeToCluster = function(cluster,node,callback){
    if(this.validateNodeJson(node,callback)){
        this.createNode(cluster,[node],"nodes",function(err){
            callback(err)
        })
    }
}

BigQueueClustersAdmin.prototype.addJournalToCluster = function(cluster,node,callback){
    if(this.validateNodeJson(node,callback)){
        this.createNode(cluster,[node],"journals",function(err){
            callback(err)
        })
    }
}

BigQueueClustersAdmin.prototype.updateNodeData = function(clusterName,node,callback){
    var self = this
    if(this.validateNodeJson(node,callback)){
        var nodePath = this.zkClustersPath+"/"+clusterName+"/nodes/"+node.name
        this.zkClient.a_get(nodePath,false,function(rc,error,stat,data){
            if(rc != 0){
                callback("Error ["+rc+" - "+error+"] on path ["+nodePath+"]")
                return
            }
            var nodeData = JSON.parse(data)
            var keys = Object.keys(node.config)
            for(var i in keys){
                var key = keys[i]
                nodeData[key] = node.config[key]
            }
            self.zkClient.a_set(nodePath,JSON.stringify(nodeData),-1,function(rc,error, stat){
                if(rc != 0){
                    callback("Error ["+rc+" - "+error+"] updating data of node on path:"+nodePath)
                }else{
                    callback()
                }
            })    
        })
    }
}


BigQueueClustersAdmin.prototype.createNode = function(clusterName,nodesData,nodeType,callback){

    if(!nodesData || nodesData.lenght == 0){
        callback()
        return
    }
    var exec = 0
    var errors = []
    var nodesPath = this.zkClustersPath+"/"+clusterName+"/"+nodeType
    for(var i in nodesData){
        var nodePath = nodesPath+"/"+nodesData[i].name
        var data = nodesData[i].config
        if(data instanceof Object){
            data = JSON.stringify(data)
        }

        this.zkClient.a_create(nodePath,data,this.zkFlags,function(rc,error,stat){
            if(rc!=0){
                errors.push(error)
            }
            exec++;
            if(exec == nodesData.length){
                if(errors.length>0){
                    callback("Error creating nodes ["+nodeType+"], "+JSON.stringify(errors))
                }else{
                    callback()
                }
            }
        })
    }

}

BigQueueClustersAdmin.prototype.createTopic = function(topic,cluster,callback){
    var self = this
    var topicName
    var group
    var groupIndexPath
    var ttl
    if(topic instanceof Object){
        topicName = topic.name
        ttl = topic.ttl
        group = topic.group
        groupIndexPath = this.zkGroupsIndexPath+"/"+group
   }else{
        topicName = topic
    }
    var topicIndexPath = this.zkTopicsIndexPath+"/"+topicName
    if(!cluster)
        cluster = this.defaultCluster
    this.zkClient.a_exists(topicIndexPath,false,function(rc,error,stat){ 
        if(rc == 0){
            callback("Topic ["+topic+"] already exist")
            return
        }
        self.getClusterClient(cluster,function(client){
            client.createTopic(topicName,ttl,function(err){
                if(err){
                    callback(err)
                    return
                }
                var indexData = {"cluster":cluster,"create_time":new Date()}
                self.zkClient.a_create(topicIndexPath,JSON.stringify(indexData),self.zkFlags,function(rc,error,path){
                    if(rc!=0){
                        callback(error+", path: "+topicIndexPath)
                        return
                    }
                    if(group){
                         self.zkClient.a_create(groupIndexPath,"",self.zkFlags,function(rc,error,path){
                            self.zkClient.a_create(groupIndexPath+"/"+topicName,"",self.zkFlags,function(rc,error,path){
                                if(rc!=0){
                                    callback(error+", path: "+groupIndexPath+"/"+topicName)
                                    return
                                }
                                callback()
                            })
                         })
                    }else{
                        callback()
                    }
                })
            })
        })
    })
}

BigQueueClustersAdmin.prototype.getClusterClient = function(cluster,callback){
    var self = this
    if(this.clusterClients[cluster]){
        callback(this.clusterClients[cluster])
        return
    }
    this.getClusterConfig(cluster,function(config){
        var client = bqc.createClusterClient(config)
        client.on("ready",function(err){
            self.clusterClients[cluster] = client
            callback(client)
        })
        client.on("error",function(err){
            log.err("Error on cluster client",err)
        })
    })
}

BigQueueClustersAdmin.prototype.getClusterConfig = function(cluster,callback){
    var self = this
    var config = {
        "zkConfig":self.zkConfig,
        "zkClusterPath":self.zkClustersPath+"/"+cluster,
        "createJournalClientFunction":self.createJournalClientFunction,
        "createNodeClientFunction":self.createNodeClientFunction
    }
    callback(config)
}

BigQueueClustersAdmin.prototype.getClusterClientForTopic = function(topic,callback){
    var topicIndexPath=this.zkTopicsIndexPath+"/"+topic
    var self = this
    this.zkClient.a_get(topicIndexPath,false,function(rc,error,stat,data){
        if(rc!=0){
            callback("Data for topic ["+topic+"] can not be found")
            return
        }
        var topicInfo = JSON.parse(data)
        self.getClusterClient(topicInfo.cluster,function(client){
            callback(undefined,client,topicInfo.cluster)
        })
    })
}


BigQueueClustersAdmin.prototype.createConsumer = function(topic,consumer,callback){
    var self = this
    this.getClusterClientForTopic(topic,function(err,client){
        if(err){
            callback(err)
            return
        }
        client.createConsumerGroup(topic,consumer,function(err){
            if(err){
                callback(err)
            }else{
                callback()
            }
        })
    })
   
}

BigQueueClustersAdmin.prototype.getTopicStats = function(topic,clusterClient,callback){
     clusterClient.getConsumerGroups(topic,function(err,consumers){
        if(err){
            callback(err)
            return
        }
        var total=consumers.length
        var executed=0
        var errors = []
        var data=[]
        if(total== 0){
            callback(undefined,[])
            return
        }
        consumers.forEach(function(consumer){
            clusterClient.getConsumerStats(topic,consumer,function(err,stats){
                executed++
                if(err){
                    errors.push(err)  
                }else{
                    var d = {"consumer":consumer}
                    d.stats=stats
                    data.push(d)
                }
                //If an error ocurs the executed never will be equals than total
                if(executed>=total){
                    if(errors.lenght>0){
                        callback(errors)
                    }else{
                        callback(undefined,data)
                    }
                }
            })
        })
    })
}

BigQueueClustersAdmin.prototype.getTopicData = function(topic,callback){
    var self = this
    this.getClusterClientForTopic(topic,function(err,client,cluster){
        if(err){
            callback(err)
            return
        }    
        client.getTopicTtl(topic,function(err,ttl){
            if(err){
                callback(err)
                return
            }
            self.getTopicStats(topic,client,function(err,consumers){
                if(err){
                    callback(err)
                    return
                }
               var clusterEntryPointsPath = self.zkClustersPath+"/"+cluster+"/entrypoints"
               utils.getDataFromChilds(self.zkClient,clusterEntryPointsPath,function(error,entrypoints){
                    if(error){
                        callback(error+", path: "+clusterEntryPointsPath)
                        return
                    }
                    var entrypoints = entrypoints
                    var topicData = {
                        "topic_id":topic,
                        "ttl":ttl,
                        "cluster":cluster,
                        "entrypoints": entrypoints,
                        "consumers":consumers
                    }
                    callback(undefined, topicData)
                })
            })
        })
    })
}

BigQueueClustersAdmin.prototype.getConsumerData = function(topic,consumer,callback){
    this.getTopicData(topic,function(err,data){
        if(err){
            callback(err)
            return;
        }
        var consumer_stats 
        data.consumers.forEach(function(val){
            if(val.consumer == consumer){
                consumer_stats = val.stats
                return
            }
        })
        delete data["consumers"]
        data["consumer_id"] = consumer
        data["consumer_stats"] = consumer_stats
        callback(undefined,data)
    })
}
 
BigQueueClustersAdmin.prototype.listClusters = function(callback){
    this.zkClient.a_get_children(this.zkClustersPath,false,function(rc,error,childrens){
        if(rc!=0){
            callback(error+", path: "+this.zkClusterPath)
        }else{
            callback(undefined,childrens)
        }
    })
}

BigQueueClustersAdmin.prototype.getClusterData = function(cluster,callback){
   var self = this
   var nodesPath = this.zkClustersPath+"/"+cluster+"/nodes"
   utils.getDataFromChilds(this.zkClient,nodesPath,function(error,nodes){
      if(error){
        callback(error+", path: "+nodesPath)
        return
      }
      var clusterEntryPointsPath = self.zkClustersPath+"/"+cluster+"/entrypoints"
      utils.getDataFromChilds(self.zkClient,clusterEntryPointsPath,function(error,entrypoints){
          if(error){
              callback(error+", path: ["+clusterEntryPointsPath+"]")
              return
          }
          self.getClusterClient(cluster,function(client){
                client.listTopics(function(topics){
                    var clusterData = {
                                        "cluster":cluster,
                                        "nodes":nodes,
                                        "topics":topics,
                                        "entrypoints":entrypoints
                                      }
                    callback(undefined,clusterData)

                })
           })
       })
   })

}

BigQueueClustersAdmin.prototype.getGroupTopics = function(group,callback){
    var groupPath = this.zkGroupsIndexPath+"/"+group
    this.zkClient.a_get_children(groupPath,false,function(rc,error,childrens){
        if(rc!=0){
            callback(error+", path: "+groupPath)
        }else{
            callback(undefined,childrens)
        }
    })
}

exports.createClustersAdminClient = function(conf){
   var zk = new ZK(conf.zkConfig)
   log.setLevel(conf.logLevel || "info")
   var bqadm = new BigQueueClustersAdmin(zk,conf.zkConfig,conf.defaultCluster,conf.zkBqPath,conf.zkFlags,conf.createNodeClientFunction,conf.createJournalClientFunction)
   return bqadm
}
