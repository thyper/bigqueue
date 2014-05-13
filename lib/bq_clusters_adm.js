var events = require("events"),
    utils = require("../lib/bq_client_utils.js"),
    log = require("node-logging"),
    mysql = require('mysql'),
    async = require("async");

function BigQueueClustersAdmin(defaultCluster, mysqlConf, defaultTtl){
    this.defaultCluster = defaultCluster
    this.shutdowned = false   
    this.mysqlPool = mysql.createPool(mysqlConf);
    this.defaultTtl = defaultTtl || 259200; //3 days 
    this.init()
}

BigQueueClustersAdmin.prototype = Object.create(require('events').EventEmitter.prototype);


BigQueueClustersAdmin.prototype.shutdown = function(){
    this.shutdowned = true
}

BigQueueClustersAdmin.prototype.init = function(){
  var self = this;
  process.nextTick(function() {
    self.emit("ready")
  });

}

BigQueueClustersAdmin.prototype.createBigQueueCluster = function(clusterData,callback){
  var self = this
  if(!clusterData.name){
      return callback({"msg":"No cluster name exists","code":404})
  }
  
  if(!clusterData.nodes) {
    clusterData.nodes = [];
  }
  
  if(!clusterData.journals) {
    clusterData.journals = [];
  }
  
  if(!clusterData.endpoints) {
    clusterData.endpoints = [];
  }
  var err = clusterData.nodes.concat(clusterData.journals).some(function(e) {
    return !self.validateNodeJson(e);
  });
  
  if(err) {
    return callback({err: "Some journal or node has incorrect parameters"});
  }
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      return callback(err);
    }
    mysqlConn.query("INSERT INTO clusters (name) VALUES (?)",[clusterData.name], function(err) {
      mysqlConn.release();
      if(err) {
        return callback(err);
      }
      async.series([
        function(endSection) {
          async.each(clusterData.journals, function(e, cb) {
            self.addJournalToCluster(clusterData.name,e, cb);
          }, endSection); 
        },
        function(endSection) {
          async.each(clusterData.nodes, function(e, cb) {
            self.addNodeToCluster(clusterData.name,e, cb);
          }, endSection); 
        },
        function(endSection) {
          async.each(clusterData.endpoints, function(e, cb) {
            self.addEndpointToCluster(clusterData.name,e, cb);
          }, endSection); 
        }

      ],function(err) {
        callback(err);
      });
    });
  });
}

BigQueueClustersAdmin.prototype.createNode = function(clusterName,nodesData,nodeType,callback){
  nodesData.options = nodesData.options ? nodesData.options : {}; 
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      return callback(err);
    }
    async.series([
      function(d) {
        mysqlConn.beginTransaction(d);
      },
      function(d) {
        mysqlConn.query("INSERT INTO data_nodes (id, host, port, status, type, options, cluster) VALUES(?,?,?,?,?,?,?)", 
                      [nodesData.id, nodesData.host, nodesData.port, nodesData.status, nodeType, JSON.stringify(nodesData.options), clusterName],d);
      },
      function(d) {
        if(nodesData.journals && nodesData.journals.length > 0) {
          async.each(nodesData.journals, function(e, cb) {
            mysqlConn.query("INSERT INTO node_journals VALUES (?,?)",[nodesData.id,e],cb);
          },d);
        } else {
          d();
        }
      },
      function(d) {
        mysqlConn.commit(d);
      }
    ], function(err) {
      if(err) {
        mysqlConn.rollback(function() {
          mysqlConn.release();
          callback(err);
        })
      } else {
        mysqlConn.release();
        callback(err);
      }
    });
  });
}

BigQueueClustersAdmin.prototype.validateNodeJson = function(node,callback){
    if(node.id == undefined ||
            node.host == undefined ||
            node.host == "" ||
            node.port == undefined ||
            node.port == "" ){
        if(callback)
          callback({"msg":"Node should have a name and a config","code":406})
        return false
    }
    return true
}

BigQueueClustersAdmin.prototype.addNodeToCluster = function(cluster,node,callback){
  if(this.validateNodeJson(node)){
        this.createNode(cluster,node,"node",function(err){
            if(err)
                err={"msg":err}
            callback(err)
        })
    } else {
      callback({err: "Data is not valid for create node"})
    }
}

BigQueueClustersAdmin.prototype.addJournalToCluster = function(cluster,node,callback){
    if(this.validateNodeJson(node)){
        this.createNode(cluster,node,"journal",function(err){
            if(err)
                err={"msg":err}

            callback(err)
        })
    } else {
      callback({err: "Data is not valid for create journal"})
    }
}

BigQueueClustersAdmin.prototype.addEndpointToCluster = function(cluster,node,callback){
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      callback(err);
    }
    mysqlConn.query("INSERT INTO endpoints (host, port, description, cluster) VALUES (?,?,?,?)",[node.host,node.port,node.description,cluster], function(err) {
      mysqlConn.release();
      callback(err);
    });
  });
}

BigQueueClustersAdmin.prototype.updateJournalData = function(clusterName,node,callback){
  this.updateGenericNodeData(clusterName, node, "journals", callback);
}
BigQueueClustersAdmin.prototype.updateNodeData = function(clusterName,node,callback){
  this.updateGenericNodeData(clusterName, node, "nodes", callback);
}
BigQueueClustersAdmin.prototype.updateGenericNodeData = function(clusterName,node,type,callback){
    var self = this
    var query = "UPDATE data_nodes SET";
    if(node.cluster || node.type || node.host || node.port) {
      return callback({err:"You can't modify primary properties, in that case create a new node and delete this"});
    }
    if(!node.id) {
      return callback({err: "id is required"});
    }
    var queryValues = [];
    Object.keys(node).forEach(function(e) {
      if(e != "journals" && e != "id") {
        query+=" ?? = ?, ";
        queryValues.push(e, node[e]);
      }
    });
    query = query.substring(0,query.lastIndexOf(", "));
    query+=" WHERE ?? = ?";
    queryValues.push("id", node.id);
    this.mysqlPool.getConnection(function(err, mysqlConn) {
      if(err) {
        return callback(err);
      }
      async.series([
        function(d) {
          mysqlConn.beginTransaction(d);
        }, function(d) {
          //If you only change journals will not update this
          if(queryValues.length > 2) { 
            mysqlConn.query(query, queryValues, d);
          } else {
            d();
          }
        }, function(d) {
          if(node.journals) {
            mysqlConn.query("DELETE FROM node_journals WHERE node_id = ?",node.id, function(err) {
              if(err) {
                d(err);
              }
              async.each(node.journals, function(e, cb) {
                mysqlConn.query("INSERT INTO node_journals VALUES (?,?)",[node.id,e],cb);
              },d);
            });
          } else {
            d();
          }
        }, function(d) {
          mysqlConn.commit(d);
        }
      ],function(err) {
        if(err) {
          mysqlConn.rollback(function() {
            mysqlConn.release();
            callback(err);
          });
        } else {
          mysqlConn.release();
          callback(err);
        }
      });
    });
}

BigQueueClustersAdmin.prototype.getDefaultCluster = function(callback) {
 this.mysqlPool.getConnection(function(err, mysqlConn) {
  if(err) {
    callback(err);
  }
  mysqlConn.query("SELECT name FROM clusters WHERE `default` = ? limit 1",["Y"], function(err, data) {
    if(err) {
      callback(err);
    } else {
      callback(undefined, data[0].name);
    }
  });
 });
}

BigQueueClustersAdmin.prototype.createTopic = function(topic,callback){
 var self = this;
 if(!topic.name || !topic.tenant_id || ! topic.tenant_name) {
  return callback({err: "name, tenant_id and tenant_name are required" });
 }
 this.mysqlPool.getConnection(function(err, mysqlConn) {
  if(err) {
    callback(err);
  }
  var id = topic.tenant_id+"-"+topic.tenant_name+"-"+topic.name;
  var cluster;
  async.series([function(cb) {
    if(topic.cluster) {
      mysqlConn.query("SELECT name FROM clusters where name = ?",[topic.cluster], function(err, data) {
       if(err) {
        cb(err);
       }else if(data.length == 0) {
        cb({"err":"Cluster "+topic.cluster+" not found"});
       } else {
         cluster = topic.cluster;
         cb();
       }
      })
    } else {
      self.getDefaultCluster(function(err, c) {
        cluster = c;
        cb(err);
      });
    }
  },
  function(cb) {
    mysqlConn.query("INSERT INTO topics (topic_id, tenant_id, tenant_name, topic_name, cluster, ttl) VALUES (?,?,?,?,?,?)",
                   [id, topic.tenant_id, topic.tenant_name, topic.name, cluster, topic.ttl || self.defaultTtl], cb);
 },
  function(cb) {
    mysqlConn.release();
    cb();
  }], function(err) {
    callback(err);
  });
 });
}

BigQueueClustersAdmin.prototype.deleteTopic = function(topic,callback){
  var self = this
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      return callback(err);
    }
    
    async.series([
      function(cb) {
        self.getTopicData(topic, function(err, data) {
          if(data && data.consumers && data.consumers.length > 0) {
            return cb({err: "Topic ["+topic+"] has consumers, please delete all consumers before delete this topic"});
          }
          cb(err);
        });  
      },
      function(cb) {
        mysqlConn.release();
        cb();
      },
      function(cb) {
        mysqlConn.query("DELETE FROM topics WHERE topic_id = ?", [topic], cb);
      }
    ], function(err) {
      callback(err);
    });
  });
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


BigQueueClustersAdmin.prototype.createConsumerGroup = function(consumer,callback){
  var self = this
  if(!consumer.name || !consumer.topic_id || !consumer.tenant_id || !consumer.tenant_name) {
    return callback("name, topic_id, tenant_id, tenant_name are required");
  }
  self.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      return callback(err);
    }
    async.series([
      function(cb) {
       //If topic doesn't exist it will get an error and the consumer will not be created 
       self.getTopicData(consumer.topic_id,cb);
      },
      function(cb) {
        var id = consumer.tenant_id+"-"+consumer.tenant_name+"-"+consumer.name;
          if(err) {
            cb(err);
          }
          mysqlConn.query("INSERT INTO consumers (consumer_id, tenant_id, tenant_name, consumer_name, topic_id) VALUES (?,?,?,?,?)",
                           [id, consumer.tenant_id, consumer.tenant_name, consumer.name, consumer.topic_id], cb);
      },
      function(cb) {
        mysqlConn.release();
        cb();
      }
    ],callback);
  });
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
    var topicData = {};
    this.mysqlPool.getConnection(function(err, mysqlConn) {
      if(err) {
        return callback(err);
      }
      async.series([
        //Base topic data
        function(cb) {
          mysqlConn.query("SELECT topic_id, ttl, cluster FROM topics where topic_id = ?", [topic], function(err, data) {
            if(data) {
              if(data.length != 1) {
                cb({err: "Topic ["+topic+"] not found"})
              } else {
                topicData = data[0];
              }
            }
            cb(err);
          });  
        },
        function(cb) {
          //Consumer data
          async.waterfall([
          function(wcb) {
            mysqlConn.query("SELECT consumer_id FROM consumers WHERE topic_id = ?", [topic],function(err, data) {
              wcb(err, data);
            });
          },
          function(consumers, wcb) {
            var consumers_arr = [];
            async.each(consumers, function(e, ecb) {
              self.getConsumerStats(topicData.cluster,topicData.topic_id, e.consumer_id, function(err,data) {
                consumers_arr.push({consumer_id: e.consumer_id, stats: data});
                ecb(err);
              });
            }, function(err) {
              wcb(err, consumers_arr);
            });
          },
          ], function(err, res) {
            topicData["consumers"] = res
            cb(err)
          });
        },
        function(cb) {
          //endpoints data
          mysqlConn.query("SELECT host, port, description FROM endpoints WHERE cluster = ?", [topicData.cluster],function(err,data) {
            topicData["endpoints"] = data;
            cb(err);
          });
        },
        function(cb) {
          mysqlConn.release();
          cb();
        }
      ], function(err) {
        callback(err, topicData);
      });
    });
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
                self.zkClient.a_get_children(clusterTopicsPath,false,function(rc,error,topics){     
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
            })
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
   log.setLevel(conf.logLevel || "info")
   var bqadm = new BigQueueClustersAdmin(conf.defaultCluster,conf.mysqlConf);
   return bqadm
}
