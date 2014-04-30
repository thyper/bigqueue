var events = require("events"),
    bqc = require("../lib/bq_cluster_client.js"),
    ZK = require("zookeeper"),
    utils = require("../lib/bq_client_utils.js"),
    log = require("node-logging"),
    async = require("async"),
    mysql = require('mysql');

function BigQueueClustersAdmin(zkClient,zkConfig,defaultCluster,zkBqPath,zkFlags,createNodeClientFunction,createJournalClientFunction, mysqlConf){
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
    this.clusterFolders = ["nodes","journals","topics","endpoints"]
    this.shutdowned = false   
    this.mysqlPool = mysql.createPool(mysqlConf);
    this.init()
}

BigQueueClustersAdmin.prototype = new events.EventEmitter()

BigQueueClustersAdmin.prototype.shutdown = function(){
    this.shutdowned = true
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
            log.err("Error connecting to zookeeper ["+err+"]")
            self.emit("error",err)
            return
        }
        self.emit("ready")
    })

}

BigQueueClustersAdmin.prototype.createBigQueueCluster = function(clusterData,callback){
    var self = this
    if(!clusterData.name){
        callback({"msg":"No cluster name exists","code":404})
        return
    }
    var clusterPath = this.zkClustersPath+"/"+clusterData.name
    this.zkClient.a_create(clusterPath,"",this.zkFlags,function(rc,error,stat){
      if(rc!=0){
            callback({"msg":error})
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
                        callback({"msg":"Error creatng clusters ["+JSON.stringify(errors)+"]"})
                    }else{
                        var exec2 = 0
                        for(var i in self.clusterFolders){
                            var label = self.clusterFolders[i]
                            self.createNode(clusterData.name,clusterData[label],label,function(err){
                                exec2++;
                                if(err){
                                    errors.push(err)
                                }
                                if(exec2 == self.clusterFolders.length){
                                    if(errors.length > 0){
                                        return callback({"msg":"Error creatng clusters ["+JSON.stringify(errors)+"]"})
                                    }
                                    callback()
                                }

                            })
                        }
                    }
                }
            })
        }
    })
}

BigQueueClustersAdmin.prototype.validateNodeJson = function(node,callback){
  if(node == undefined ||
            node.id == undefined || node == undefined ||
            node.host == undefined ||
            node.host == "" ||
            node.port == undefined ||
            node.port == "" ){
        callback({"msg":"Node should have a id, host and port","code":406})
        return false
    }
    return true
}

BigQueueClustersAdmin.prototype.addNodeToCluster = function(cluster,node,callback){
  if(this.validateNodeJson(node,callback)){
        this.createNode(cluster,[node],"nodes",function(err){
            if(err)
                err={"msg":err}
            callback(err)
        })
    }
}

BigQueueClustersAdmin.prototype.addJournalToCluster = function(cluster,node,callback){
    if(this.validateNodeJson(node,callback)){
        this.createNode(cluster,[node],"journals",function(err){
            if(err)
                err={"msg":err}

            callback(err)
        })
    }
}

BigQueueClustersAdmin.prototype.addEntrypointToCluster = function(cluster,node,callback){
    if(this.validateNodeJson(node,callback)){
        this.createNode(cluster,[node],"endpoints",function(err){
            if(err)
                err={"msg":err}

            callback(err)
        })
    }
}

BigQueueClustersAdmin.prototype.updateNodeData = function(clusterName,node,callback){
    var self = this
    if(!node || !node.id) {
      return callback({"msg":"id is required"});
    }
    var nodePath = this.zkClustersPath+"/"+clusterName+"/nodes/"+node.id
    this.zkClient.a_get(nodePath,false,function(rc,error,stat,data){
        if(rc != 0){
            callback({"msg":"Error ["+rc+" - "+error+"] on path ["+nodePath+"]"})
            return
        }
        var nodeData = JSON.parse(data)
        var keys = Object.keys(node)
        for(var i in keys){
            var key = keys[i]
            nodeData[key] = node[key]
        }
         /*
         * Before any run we'll cehck if the data is complete
         */
        if(!utils.checkNodeData(nodeData)){
            return callback({"msg":"Node data uncomplete [ "+JSON.stringify(nodeData)+" ] [host,port,status,journals are required]"})
        }
        self.zkClient.a_set(nodePath,JSON.stringify(nodeData),-1,function(rc,error, stat){
            if(rc != 0){
                callback({"msg":"Error ["+rc+" - "+error+"] updating data of node on path:"+nodePath})
            }else{
                callback()
            }
        })
    })
}


BigQueueClustersAdmin.prototype.createNode = function(clusterName,nodesData,nodeType,callback){

    if(!nodesData || nodesData.length == 0){
        callback()
        return
    }
    var exec = 0
    var errors = []
    var nodesPath = this.zkClustersPath+"/"+clusterName+"/"+nodeType
    for(var i in nodesData){
        var nodePath = nodesPath+"/"+nodesData[i].id
        var data = nodesData[i]
        if(nodeType == "nodes" && !utils.checkNodeData(data)){
          return callback({"msg":"Node data uncomplete [ "+JSON.stringify(data)+" ] [host,port,status,journals are required]"})
        }else if(nodeType == "journals" && !utils.checkJournalData(data)){
        }
        this.zkClient.a_create(nodePath,JSON.stringify(data),this.zkFlags,function(rc,error,stat){
            if(rc!=0){
                errors.push(error)
            }
            exec++;
            if(exec == nodesData.length){
                if(errors.length>0){
                    callback({"msg":"Error creating nodes ["+nodeType+"], "+JSON.stringify(errors)})
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
            callback({"msg":"Topic ["+topic+"] already exist","code":409})
            return
        }
        self.getClusterClient(cluster,function(client){
            if(!client){
                callback({"msg":"Error getting client for ["+JSON.stringify(cluster)+"]"})
                return
            }
            client.createTopic(topicName,ttl,function(err){
                if(err){
                    callback(err)
                    return
                }
                var indexData = {"cluster":cluster,"create_time":new Date(),"group":group}
                self.zkClient.a_create(topicIndexPath,JSON.stringify(indexData),self.zkFlags,function(rc,error,path){
                    if(rc!=0){
                        callback({"msg":error+", path: "+topicIndexPath})
                        return
                    }
                    if(group){
                         self.zkClient.a_create(groupIndexPath,"",self.zkFlags,function(rc,error,path){
                            self.zkClient.a_create(groupIndexPath+"/"+topicName,"",self.zkFlags,function(rc,error,path){
                                if(rc!=0){
                                    callback({"msg":error+", path: "+groupIndexPath+"/"+topicName})
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

BigQueueClustersAdmin.prototype.deleteTopic = function(topic,callback){
    var self = this
    var topicIndexPath = this.zkTopicsIndexPath+"/"+topic
    this.zkClient.a_exists(topicIndexPath,false,function(rc,error,stat){
        if(rc!=0){
            return callback({"msg":"Topic not found, "+rc+" - "+error,"code":404})
        }
        self.zkClient.a_get(topicIndexPath,false,function(rc,error,stat,data){
            var topicData = JSON.parse(data)
            if(rc != 0)
                return callback({"msg":"Topic ["+topic+"] doesn't exist","code":404})
            self.getClusterClientForTopic(topic,function(err,client){
                if(!client || err)
                    return callback({"msg":"Error getting client for ["+JSON.stringify(cluster)+"]"})
                client.deleteTopic(topic,function(err){
                    if(err){
                        return callback(err)
                    }
                    var groupIndexPath
                    if(topicData.group)
                        groupIndexPath = self.zkGroupsIndexPath+"/"+topicData.group+"/"+topic
                    process.nextTick(function(){
                        self.zkClient.a_delete_(topicIndexPath,false,function(rc,error){
                            if(rc!=0)
                                return callback({"msg":"Error deleting ["+topicIndexPath+"] from zookeeper, "+rc+" - "+error})
                            if(groupIndexPath){
                                self.zkClient.a_delete_(groupIndexPath,false,function(rc,error){
                                    if(rc!=0)
                                        return callback({"msg":"Error deleting ["+groupIndexPath+"] from zookeeper, "+rc+" - "+error})
                                    callback()
                                })
                            }else{
                                callback()
                            }
                        })
                    })
                })
            })
        })
    })

}

BigQueueClustersAdmin.prototype.getClusterClient = function(cluster,callback){
    var self = this
    var sent = false;
    if(this.clusterClients[cluster]){
        return callback(this.clusterClients[cluster])
    }
    this.getClusterConfig(cluster,function(config){
        var client = bqc.createClusterClient(config)
        client.on("ready",function(err){
            self.clusterClients[cluster] = client
            if(!sent){
                callback(client)
                sent = true
            }
        })
        client.on("error",function(err){
            delete self.clusterClients[cluster]
            if(!sent){
                callback(undefined)
                sent = true
            }
            log.err("Error on cluster client of cluster ["+cluster+"]",err)

        })
    })
}

BigQueueClustersAdmin.prototype.getClusterConfig = function(cluster,callback){
    var self = this
    var config = {
        "zk":self.zkClient,
        "zkClusterPath":self.zkClustersPath+"/"+cluster,
        "createJournalClientFunction":self.createJournalClientFunction,
        "createNodeClientFunction":self.createNodeClientFunction
    }
    callback(config)
}

BigQueueClustersAdmin.prototype.getClusterClientForTopic = function(topic,callback){
    var topicIndexPath=this.zkTopicsIndexPath+"/"+topic
    var self = this
    if(this.shutdowned)
        return callback({"msg":"Client shutdowned"})

    this.zkClient.a_exists(topicIndexPath,false,function(rc,error){
        if(rc!=0){
            return callback({"msg":"Error getting ["+topicIndexPath+"], "+rc+" - "+error,"code":404})
        }
        self.zkClient.a_get(topicIndexPath,false,function(rc,error,stat,data){
            if(rc!=0){
                callback({"msg":"Data for topic ["+topic+"] can not be found, at path ["+topicIndexPath+"]","code":404})
                return
            }
            var topicInfo = JSON.parse(data)
            self.getClusterClient(topicInfo.cluster,function(client){
                callback(undefined,client,topicInfo.cluster,topicInfo.group)
            })
        })
    })
}


BigQueueClustersAdmin.prototype.createConsumerGroup = function(topic,consumer,callback){
    var self = this
    this.getClusterClientForTopic(topic,function(err,client){
        if(err){
            err={"msg":err}
            callback(err)
            return
        }
        client.createConsumerGroup(topic,consumer,function(err){
            if(err){
                err={"msg":err}
                callback(err)
            }else{
                callback()
            }
        })
    })

}

BigQueueClustersAdmin.prototype.deleteConsumerGroup = function(topic,consumer,callback){
    var self = this
    this.getClusterClientForTopic(topic,function(err,client){
        if(err){
            callback(err)
            return
        }
        client.deleteConsumerGroup(topic,consumer,function(err){
            if(err){
                callback(err)
            }else{
                callback()
            }
        })
    })
}

BigQueueClustersAdmin.prototype.resetConsumerGroup = function(topic,consumer,callback){
    var self = this
    this.getClusterClientForTopic(topic,function(err,client){
        if(err){
            callback(err)
            return
        }
        client.resetConsumerGroup(topic,consumer,function(err){
            if(err){
                callback(err)
            }else{
                callback()
            }
        })
    })
}

BigQueueClustersAdmin.prototype.getTopicStats = function(topic,cluster,callback){
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      if(mysqlConn) {
        mysqlConn.release();
      }
      return callback(err);
    }
    mysqlConn.query("SELECT consumer, sum(lag) as lag, sum(fails) as fails, sum(processing) as processing "+ 
                    "FROM stats "+ 
                    "WHERE cluster = ? AND topic = ? "+
                    "GROUP BY cluster, topic, consumer", [cluster, topic], function(err, data) {
        var ret;
        if(data) {
          ret = [];
          data.forEach(function(val) {
            ret.push({consumer_id: val.consumer, consumer_stats: {lag: val.lag, fails: val.fails, processing: val.processing}});
          });
        }
       mysqlConn.release();
        return callback(err, ret);
    });
  });
}


BigQueueClustersAdmin.prototype.getTopicData = function(topic,callback){
    var self = this
    var consumersObj = {};
    var consumers = [];
    this.getClusterClientForTopic(topic,function(err,client,cluster,group){
      if(err){
            callback(err)
            return
        }
        client.getTopicTtl(topic,function(err,ttl){
            if(err){
                callback(err)
                return
            }
            client.getConsumerGroups(topic, function(err, consumerGroups) {
              if(err) {
                return callback(err);
              }
              consumerGroups.forEach(function(consumer) {
                consumersObj[consumer] = {consumer_id: consumer};
              });
              self.getTopicStats(topic,cluster,function(err,consumerStats){
                 if(err){
                     callback(err)
                     return
                 }
                 consumerStats.forEach(function(elem) {
                  if(consumersObj[elem.consumer_id]) {
                    consumersObj[elem.consumer_id].stats = elem.consumer_stats; 
                  }
                 });
                 Object.keys(consumersObj).forEach(function(key) {
                   consumers.push(consumersObj[key]);
                 });
                 var clusterEntryPointsPath = self.zkClustersPath+"/"+cluster+"/endpoints"
                 utils.getDataFromChilds(self.zkClient,clusterEntryPointsPath,function(error,endpoints){
                      if(error){
                          callback({"msg":error+", path: "+clusterEntryPointsPath,"code":404})
                          return
                      }
                      var endpoints = endpoints
                      var topicData = {
                          "topic_id":topic,
                          "ttl":ttl,
                          "cluster":cluster,
                          "endpoints": endpoints,
                          "consumers":consumers
                      }
                      callback(undefined, topicData)
                  })
              })
            });
        })
    })
}

BigQueueClustersAdmin.prototype.getConsumerStats = function(cluster, topic, consumer, callback) {
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      if(mysqlConn) {
        mysqlConn.release();
      }
      return callback(err);
    }
    mysqlConn.query("SELECT sum(lag) as lag, sum(fails) as fails, sum(processing) as processing "+ 
                    "FROM stats "+ 
                    "WHERE cluster = ? AND topic = ? AND consumer = ?"+
                    "GROUP BY cluster, topic, consumer", [cluster, topic, consumer], function(err, data) {
        mysqlConn.release();
        if(!data || data.length == 0) {
          return callback({"err":"No data found for ["+cluster+"]["+topic+"]["+consumer+"]"});
        }
        return callback(err, data[0]);
    });
  });

}

BigQueueClustersAdmin.prototype.getConsumerData = function(topic,consumer,callback){
    var self = this;
    this.getClusterClientForTopic(topic,function(err,client,cluster,group){
        if(err){
            callback(err)
            return
          }
        client.getTopicTtl(topic,function(err,ttl){
          if(err){
              callback(err)
              return
          }

          var clustersEntryPointsPath = self.zkClustersPath+"/"+cluster+"/endpoints";
          utils.getDataFromChilds(self.zkClient, clustersEntryPointsPath, function(error, endpoints){
            if(error){
                callback({"msg":error+", path: "+clustersEntryPointsPath,"code":404})
                return
            }
            self.getConsumerStats(cluster, topic, consumer, function(err,stats) {
              var data = {
                  "topic_id": topic,
                  "ttl": ttl,
                  "cluster": cluster,
                  "endpoints": endpoints,
                  "consumer_id": consumer,
                  "consumer_stats": stats}
              callback(undefined, data)
              });
          });
        });
    });    
}

BigQueueClustersAdmin.prototype.listClusters = function(callback){
    this.zkClient.a_get_children(this.zkClustersPath,false,function(rc,error,childrens){
        if(rc!=0){
            callback({"msg":error+", path: "+this.zkClusterPath})
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
        callback({"msg":error+", path: "+nodesPath})
        return
      }
      var clusterEntryPointsPath = self.zkClustersPath+"/"+cluster+"/endpoints"
      utils.getDataFromChilds(self.zkClient,clusterEntryPointsPath,function(error,endpoints){
          if(error){
              callback({"msg":error+", path: ["+clusterEntryPointsPath+"]"})
              return
          }
          var clusterJournalsPath = self.zkClustersPath+"/"+cluster+"/journals"
          utils.getDataFromChilds(self.zkClient,clusterJournalsPath,function(error,journals){
                if(error){
                    callback({"msg":error+", path: ["+clusterJournalsPath+"]"})
                    return
                }
                 var clusterTopicsPath = self.zkClustersPath+"/"+cluster+"/topics"
                 self.zkClient.a_get(clusterTopicsPath,false,function(rc,error,stat,topics){
                      async.map(topics, function(e,cb) {
                      self.getTopicData(e,cb);
                      }, function(err, topicData) {
                      if(err) {
                        return callback(err);
                      }
                      var clusterData = {
                          "cluster":cluster,
                          "nodes":nodes,
                          "journals":journals,
                          "topics":topicData,
                          "endpoints":endpoints
                        }
                       callback(undefined,clusterData)
                 })
              });
          })
       })
   })

}

BigQueueClustersAdmin.prototype.getNodeDataByType = function(cluster,node,type,callback){
    var self = this
    var nodePath = this.zkClustersPath+"/"+cluster+"/"+type+"/"+node
    this.zkClient.a_exists(nodePath,false,function(rc,error){
        if(rc!=0){
            return callback({"msg":"Error getting ["+type+": "+node+"], "+rc+" - "+error,"code":404})
        }
        self.zkClient.a_get(nodePath,false,function(rc,error,stat,data){
            if(rc!=0){
                callback({"msg":"Data for "+type+" ["+node+"] can not be found, at path ["+nodePath+"]","code":404})
                return
            }
           var data = JSON.parse(data)
           if(!data.id)
                data.id = node
           callback(undefined,data)
        })
    })

}

BigQueueClustersAdmin.prototype.getNodeData = function(cluster,node,callback){
    this.getNodeDataByType(cluster,node,"nodes",callback)
}

BigQueueClustersAdmin.prototype.getJournalData = function(cluster,journal,callback){
    this.getNodeDataByType(cluster,journal,"journals",callback)
}

BigQueueClustersAdmin.prototype.getTopicGroup = function(topic,callback){
    this.getClusterClientForTopic(topic,function(error,client,cluster,group){
        callback(error,group)
    })
}

BigQueueClustersAdmin.prototype.getGroupTopics = function(group,callback){
    var groupPath = this.zkGroupsIndexPath+"/"+group
    var self = this
    if(this.shutdowned)
        return callback({"msg":"Client shutdowned"})

    this.zkClient.a_exists(groupPath,false,function(rc,error){
        if(rc != 0){
            callback("Path ["+groupPath+"], doesn't exists")
            return
        }
        self.zkClient.a_get_children(groupPath,false,function(rc,error,childrens){
            if(rc!=0){
                callback({"msg":error+", path: "+groupPath})
            }else{
                callback(undefined,childrens)
            }
        })
    })
}

BigQueueClustersAdmin.prototype.updateNodeMetrics = function(cluster,node,stats, callback) {
  if(!stats || !stats.topic_stats) {
    return callback({err: "Invalid data for stats"});
  }
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      if(mysqlConn) {
        mysqlConn.release();
      }
      return callback(err);
    }
    mysqlConn.beginTransaction(function(err) {
      var lastCheck = stats.sample_time;
      var totalCalls = Object.keys(stats.topic_stats).length;
      var calls = 0;
      var finished = false;
      function checkFinish(err) {
        if(err && !finished) {
          finished = true;
          mysqlConn.rollback();
          mysqlConn.release();
          callback(err);
        }
        if(calls == totalCalls && !finished) {
        
          finished = true;
          mysqlConn.commit(function(err) {
            mysqlConn.release();
            callback(err);
          });
        }
      }

      checkFinish();
      Object.keys(stats.topic_stats).forEach(function(topic) {
        calls++;
        var topicStats = stats.topic_stats[topic];
        totalCalls += Object.keys(topicStats).length;
        checkFinish();
        Object.keys(topicStats).forEach(function(consumer) {
          var consumerStats = topicStats[consumer];
          if(!stats.sample_date.getTime) {
            stats.sample_date = new Date(stats.sample_date);
          }
          mysqlConn.query("INSERT INTO stats (cluster, node, topic, consumer, lag, fails, processing, last_update) VALUES (?, ?, ? ,? ,?, ? ,? ,?) "+ 
                          "ON DUPLICATE KEY UPDATE lag = values(lag), fails = values(fails), processing = values(processing), last_update = values(last_update)",
                          [cluster, node, topic, consumer, consumerStats.lag, consumerStats.fails, consumerStats.processing, stats.sample_date],function(err, data) {
              calls++;
              checkFinish(err);
          });
        });
      });
    });
  });
}

exports.createClustersAdminClient = function(conf){
   var zk = new ZK(conf.zkConfig)
   log.setLevel(conf.logLevel || "info")
   var bqadm = new BigQueueClustersAdmin(zk,conf.zkConfig,conf.defaultCluster,conf.zkBqPath,conf.zkFlags,conf.createNodeClientFunction,conf.createJournalClientFunction,conf.mysqlConf);
   return bqadm
}
