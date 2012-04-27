var events = require("events"),
    bqc = require("../lib/bq_cluster_client.js"),
    ZK = require("zookeeper"),
    ZKMonitor = require("../lib/zk_monitor.js"),
    utils = require("../lib/bq_client_utils.js")
    log = require("node-logging")



function BigQueueClustersAdmin(zkClient,zkConfig,defaultCluster,zkBqPath,zkFlags,createNodeClientFunction,createJournalClientFunction){
    this.clusterClients = []
    this.zkClient = zkClient
    this.zkConfig = zkConfig
    this.defaultCluster = defaultCluster
    this.zkFlags = zkFlags || 0
    this.zkBqPath = zkBqPath
    this.zkTopicsIndexPath = zkBqPath+"/admin/indexes/topics"
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
   var topicIndexPath=this.zkTopicsIndexPath+"/"+topic
    if(!cluster)
        cluster = this.defaultCluster
    this.zkClient.a_exists(topicIndexPath,false,function(rc,error,stat){ 
        if(rc == 0){
            callback("Topic ["+topic+"] already exist")
            return
        }
        self.getClusterClient(cluster,function(client){
            client.createTopic(topic,function(err){
                if(err){
                    callback(err)
                    return
                }
                var indexData = {"cluster":cluster,"create_time":new Date()}
                self.zkClient.a_create(topicIndexPath,JSON.stringify(indexData),self.zkFlags,function(rc,error,path){
                    if(rc!=0){
                        callback(error)
                        return
                    }
                    callback()
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
        self.getTopicStats(topic,client,function(err,consumers){
            if(err){
                callback(err)
                return
            }
           var clusterEntryPointsPath = self.zkClustersPath+"/"+cluster+"/entrypoints"
           utils.getDataFromChilds(self.zkClient,clusterEntryPointsPath,function(error,entrypoints){
                if(error){
                    callback(error)
                    return
                }
                var entrypoints = entrypoints
                var topicData = {
                    "topic":topic,
                    "cluster":cluster,
                    "entrypoints": entrypoints,
                    "consumers":consumers
                }
                callback(undefined, topicData)
            })
        })
    })
}

BigQueueClustersAdmin.prototype.getClusterData = function(cluster,callback){
   var self = this
   var nodesPath = this.zkClustersPath+"/"+cluster+"/nodes"
   utils.getDataFromChilds(this.zkClient,nodesPath,function(error,nodes){
      if(error){
        callback(error)
        return
      }
      var clusterEntryPointsPath = self.zkClustersPath+"/"+cluster+"/entrypoints"
      utils.getDataFromChilds(self.zkClient,clusterEntryPointsPath,function(error,entrypoints){
          if(error){
              callback(error)
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

exports.createClustersAdminClient = function(conf){
   var zk = new ZK(conf.zkConfig)
   log.setLevel(conf.logLevel || "info")
   var bqadm = new BigQueueClustersAdmin(zk,conf.zkConfig,conf.defaultCluster,conf.zkBqPath,conf.zkFlags,conf.createNodeClientFunction,conf.createJournalClientFunction)
   return bqadm
}
