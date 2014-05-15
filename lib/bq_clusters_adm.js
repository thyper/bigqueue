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
 self.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      return callback(err);
    }
    async.series([
      function(cb) {
          if(err) {
            cb(err);
          }
          mysqlConn.query("DELETE FROM consumers WHERE consumer_id = ? and topic_id = ?",[consumer, topic], function(err, data) {
            if(!err) {
              if(data.affectedRows != 1) {
                return cb({err: "Consumer can't be deleted [NOT_FOUND]"})
              }
            }
            cb(err);
          })
      }
    ],function(err) {
      mysqlConn.release();
      callback(err);
    });
  });
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

/**
 * Get all endpoints for an specific cluster
 */
BigQueueClustersAdmin.prototype.getEndpoinsForCluster = function(cluster, callback) {
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      return callback(err);
    }
    mysqlConn.query("SELECT host, port, description FROM endpoints WHERE cluster = ?", [cluster],function(err,data) {
      callback(err, data);
    });
  });
}

BigQueueClustersAdmin.prototype.getTopicDataByCriteria = function(criteria, callback) {
  var self = this;
  var topics = [];
  var query = "SELECT topic_id, ttl, cluster FROM topics t WHERE "
  var criteria_elements = [];
  var consumers_arr = [];
  Object.keys(criteria).forEach(function(e) {
    query+="?? = ? AND ";
    criteria_elements.push(e, criteria[e]);
  });
  query = query.substring(0, query.lastIndexOf(" AND"));
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      return callback(err);
    }
    async.waterfall([
      //Base topic data
      function(cb) {
        mysqlConn.query(query, criteria_elements, function(err, data) {
          cb(err, data);
        });  
      },
      function(topics_db, cb) {
        //Consumer data
        async.each(topics_db, function(topic, cb_each) {
          self.getConsumerByCriteria({"t.topic_id":topic.topic_id}, function(err, data) {
            if(err) {
              cb_each(err);  
            } else {
              var consumers_arr = [];
              data.forEach(function(consumer) {
                consumers_arr.push({consumer_id: consumer.consumer_id, consumer_stats: consumer.consumer_stats});
              });
              topics.push({topic_id: topic.topic_id, cluster: topic.cluster, ttl: topic.cluster, consumers: consumers_arr});
              cb_each();
            }
          })
        }
        , function(err) {
          cb(err);
        });
      }
      ], 
      function(err, res) {
        mysqlConn.release();
        callback(err, topics);
      });
  });
}
        
/**
 * Get full topic data (topic, consumers and cluster data)
 */
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
          self.getTopicDataByCriteria({topic_id: topic}, function(err, data) {
            if(data && data.length != 1) {
              return cb({err: "Topic ["+topic+"] not found"});
            }
            topicData = data[0];
            cb(err);
          });
        },
        function(cb) {
         //endpoints data
         self.getEndpoinsForCluster(topicData.cluster, function(err, data) {
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

BigQueueClustersAdmin.prototype.getConsumerByCriteria = function(criteria, callback) {
  var self = this
  var consumers = [];
  var criteria_elems = [];
  var query = "SELECT c.consumer_id consumer_id, t.topic_id topic_id, t.ttl ttl, "+ 
   "t.cluster cluster,sum(lag) as lag, "+
   "sum(fails) as fails, sum(processing) as processing "+
   "FROM consumers c " +
   "JOIN topics t "+
   "ON(t.topic_id = c.topic_id) "+
   "LEFT OUTER JOIN stats s "+
   "ON(s.consumer = c.consumer_id) ";
 
  if(criteria) {
    query+="WHERE ";
    Object.keys(criteria).forEach(function(e) {
      query+="?? = ? AND ";
      criteria_elems.push(e, criteria[e]);
    });
    query = query.substring(0, query.lastIndexOf(" AND"));
  }
  query+=" GROUP BY c.consumer_id , t.topic_id , t.ttl , t.cluster";
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      return callback(err);
    }
    mysqlConn.query(query, criteria_elems, function(err, data) {
      mysqlConn.release();
      if(err) {
        return callback(err);
      }
      data.forEach( function(e){
        if(e.consumer_id)
          consumers.push({topic_id: e.topic_id, consumer_id: e.consumer_id, ttl: e.ttl, 
                         cluster: e.cluster, 
                         consumer_stats: {lag: e.lag, fails: e.fails, processing: e.processing}});
      });
      callback(err, consumers);
    });
   });
}

BigQueueClustersAdmin.prototype.getGroupTopics = function(group,callback){
  this.getConsumerByCriteria({"t.topic_id": group}, function(err, data) {
      callback(err, data);
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
                    "WHERE cluster = ? AND topic = ? AND consumer = ? "+
                    "GROUP BY cluster, topic, consumer", [cluster, topic, consumer], function(err, data) {
        mysqlConn.release();
        if(!data || data.length == 0) {
          return callback(undefined, {});
        }
        return callback(err, data[0]);
    });
  });

}

BigQueueClustersAdmin.prototype.getConsumerData = function(topic,consumer,callback){
  this.getConsumerByCriteria({"t.topic_id": topic, "c.consumer_id": consumer}, function(err, data) {
    if(data && data.length != 1) {
      return callback({err: "Consumer ["+consumer+"] for topic ["+topic+"] not found"});
    }
    callback(err, data[0]);
  });
}

BigQueueClustersAdmin.prototype.listClusters = function(callback){
 
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    mysqlConn.query("SELECT name FROM clusters", function(err, data) {
      callback(err, data);
      mysqlConn.release();
    });
  });
}

BigQueueClustersAdmin.prototype.getClusterData = function(cluster,callback){
  var self = this

  var cluster_data = {cluster: cluster};

  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      return callback(err);
    }
    async.series([
      function(cb) {
        mysqlConn.query("SELECT name FROM clusters WHERE name = ?", [cluster], function(err, data) {
          if(data && data.length != 1) {
            return cb({err:"Cluster not found"});
          }
          cb(err);
        });
      },
      function(cb) {
        //GET NODES 
        mysqlConn.query("SELECT dn.id id, dn.host host, dn.port port, dn.status status, dn.type type, dn.options options, nj.journal_id journal "+
                        "FROM data_nodes dn " +
                        "LEFT OUTER JOIN node_journals nj " +
                        "ON (dn.id = nj.node_id) " +
                        "WHERE dn.cluster = ? " +
                        "order by dn.id", [cluster], function(err, data) {
          var nodes_data = {node:[], journal:[]};
          var actual = {};
          if(err) {
            cb(err);
          } else {
            data.forEach(function(elem) {
              if(elem.id != actual.id) {
                if(actual.id) {
                  nodes_data[actual.type].push(actual); 
                }
                actual = elem;
                if(actual.type == "node") {
                  actual.journals = [];
                  if(actual.journal) {
                    actual.journals.push(actual.journal);
                  }
                  delete actual["jorunal"]
                }
              } else {
                if(elem.journal) {
                  actual.journal.push(elem.journal);
                }
              }
            });
            cluster_data["nodes"] = nodes_data["node"];
            cluster_data["journals"] = nodes_data["journal"];
            cb();
          }
        });
      },
      function(cb) {
        //GET ENDPOINTS
       self.getEndpoinsForCluster(cluster, function(err, data) {
         cluster_data["endpoints"] = data;
         cb(err);
       });
      },
      function(cb) {
        //GET TOPICS
        self.getTopicDataByCriteria({cluster: cluster}, function(err, data) {
          cluster_data["topics"] = data;
          cb(err, data);
        });
      }
    ], function(err) {
      mysqlConn.release();
      callback(err, cluster_data);
    });
  });
}

BigQueueClustersAdmin.prototype.getNodeDataByType = function(cluster,node,type,callback){
  var self = this
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      return callback(err);
    }
    var node_data;
    
    async.series([
      function(cb) {
      mysqlConn.query("SELECT id, host, port, status, options FROM data_nodes WHERE type = ? and cluster = ? and id = ?", [type, cluster, node], function(err,data) {
           if(err) {
             return cb(err);
           }
           if(data.length != 1) {
             cb({err:"Node ["+node+"] of type ["+type+"] not found for cluster ["+cluster+"]"});
           } else {
             node_data = data[0] 
             cb();
           }
         });
        },
        function(cb) {
          if(type == "node") {
            mysqlConn.query("SELECT journal_id FROM node_journals WHERE node_id = ?", [node], function(err, data) {
              node_data["journals"] = data;
              cb(err);
            });
          } else {
           cb();
          } 
        }
      ], function(err) {
        callback(err, node_data);
      });
  });
}

BigQueueClustersAdmin.prototype.getNodeData = function(cluster,node,callback){
    this.getNodeDataByType(cluster,node,"node",callback)
}

BigQueueClustersAdmin.prototype.getJournalData = function(cluster,journal,callback){
    this.getNodeDataByType(cluster,journal,"journal",callback)
}

BigQueueClustersAdmin.prototype.getTopicGroup = function(topic,callback){
    this.getClusterClientForTopic(topic,function(error,client,cluster,group){
        callback(error,group)
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
