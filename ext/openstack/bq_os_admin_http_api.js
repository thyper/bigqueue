var express = require('express'),
    log = require('node-logging'),
    bqAdm = require('../../lib/bq_clusters_adm.js'),
    keystoneMiddlware = require("../../ext/openstack/keystone_middleware.js"),
    YAML = require('json2yaml');


var loadApp = function(app){
    var cache = app.settings.topicDataCache;
    var authorizeTenant = function(userData,tenantId){
        var authorized = false
        try{
            var tenant = userData.access.token.tenant
            if(tenant && tenant.id == tenantId){
                authorized = true
            }
        }catch(e){
            //Property doesn't exist
        }
        return authorized
    }

    var isAdmin = function(userData){
        var idToFind = app.settings.adminRoleId
        var found = false
        var roles = userData.access.user.roles
        if(roles){
            roles.forEach(function(val){
                if(val.id == idToFind){
                    found = true
                    return
                }
            })
        }
        return found
    }

    function initializeCache(topic) {
      cache[topic] = {lastVisit: new Date().getTime()};
    }

    function updateVisited(topic) {
      if(cache[topic] && cache[topic].lastVisit) {
        cache[topic].lastVisit =  new Date().getTime();
      }
    }

    app.get(app.settings.basePath+"/clusters",function(req,res){
        app.settings.bqAdm.listClusters(function(err,clusters){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty(clusters,200)
        })
    })

    app.post(app.settings.basePath+"/clusters",function(req,res){
        if(!req.is("json")){    
            return res.writePretty({err:"Error parsing json"},400)
        }
        app.settings.bqAdm.createBigQueueCluster(req.body,function(err){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty({"cluster":req.body.name},201)
        })
    })

    app.post(app.settings.basePath+"/clusters/:cluster/nodes",function(req,res){
        if(!req.is("json")){    
            return res.writePretty({err:"Error parsing json"},400)
        }
        if(!req.body.name){
            return res.writePretty({err:"Node should contains name"},400)
        }
        app.settings.bqAdm.addNodeToCluster(req.params.cluster,req.body,function(err){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty({"cluster":req.body.name},201)
        })
    })

    app.post(app.settings.basePath+"/clusters/:cluster/journals",function(req,res){
        if(!req.is("json")){    
            return res.writePretty({err:"Error parsing json"},400)
        }
        if(!req.body.name){
            return res.writePretty({err:"Node should contains name"},400)
        }
        app.settings.bqAdm.addJournalToCluster(req.params.cluster,req.body,function(err){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty({"cluster":req.body.name},201)
        })
    })

    app.post(app.settings.basePath+"/clusters/:cluster/endpoints",function(req,res){
        if(!req.is("json")){    
            return res.writePretty({err:"Error parsing json"},400)
        }
        if(!req.body.name){
            return res.writePretty({err:"Node should contains name"},400)
        }
        app.settings.bqAdm.addEntrypointToCluster(req.params.cluster,req.body,function(err){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty({"cluster":req.body.name},201)
        })
    })


    app.put(app.settings.basePath+"/clusters/:cluster/nodes/:node",function(req,res){
        if(!req.is("json")){    
            return res.writePretty({err:"Error parsing json"},400)
        }
        var node = req.body
        node["name"] = req.params.node 
        app.settings.bqAdm.updateNodeData(req.params.cluster,node,function(err){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty({"cluster":req.body.name},200)
        })
    })
    
    app.get(app.settings.basePath+"/clusters/:cluster/nodes/:node",function(req,res){
        app.settings.bqAdm.getNodeData(req.params.cluster,req.params.node,function(err,data){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty(data,200)
        })
    })
    
    app.get(app.settings.basePath+"/clusters/:cluster/journals/:journal",function(req,res){
        app.settings.bqAdm.getJournalData(req.params.cluster,req.params.journal,function(err,data){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty(data,200)
        })
    })

    app.get(app.settings.basePath+"/clusters/:cluster",function(req,res){
        app.settings.bqAdm.getClusterData(req.params.cluster,function(err,data){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty(data,200)
        })
    })

    app.get(app.settings.basePath+"/topics",function(req,res){
        var group = req.query[app.settings.groupEntity]
        if(!group){
            return res.writePretty({err:"The parameter ["+app.settings.groupEntity+"] must be set"},400)
        }
        app.settings.bqAdm.getGroupTopics(group,function(err,data){
           if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
           }
            return res.writePretty(data,200)
        })
    })

    app.post(app.settings.basePath+"/topics",function(req,res){
        if(!req.is("json")){    
            return res.writePretty({err:"Error parsing json"},400)
        }
        var group = req.body[app.settings.groupEntity]

        if(!group){
            return res.writePretty({"err":"The property ["+app.settings.groupEntity+"] must be set"},400)
        }
        if(!req.body.name){
            return res.writePretty({"err":"The property [name] must be set"},400)
        }

        if(req.keystone && req.keystone.authorized && !authorizeTenant(req.keystone.userData, group) && !isAdmin(req.keystone.userData)){
            return res.writePretty({"err":"Invalid token for tenant ["+group+"]"},401)
        }
        if(!req.body.name){
            return res.writePretty({err:"Topics should contains a name"},400)
        }
        var topic = group+"-"+req.body.name
        var ttl = req.body.ttl
        if(ttl && ttl > app.settings.maxTtl){
            return res.writePretty({"err":"Max ttl exceeded, max ttl possible: "+app.settings.maxTtl},406)
        }
        app.settings.bqAdm.createTopic({"name":topic,"group":group,"ttl":ttl},req.body.cluster,function(err){
            if(err){
              var errMsg = err.msg || ""+err
              return res.writePretty({"err":errMsg},err.code || 500)
            }else{
                app.settings.bqAdm.getTopicData(topic,function(err,data){
                    if(err){
                      var errMsg = err.msg || ""+err
                      return res.writePretty({"err":errMsg},err.code || 500)
                    }
                    return res.writePretty(data,201)
                })
            }

        })
    })
   
    app.delete(app.settings.basePath+"/topics/:topicId",function(req,res){
        app.settings.bqAdm.getTopicGroup(req.params.topicId,function(err,group){
            if(err){
               var errMsg = err.msg || ""+err
               return res.writePretty({"err":errMsg},err.code || 500)
            }
        
            if(req.keystone && req.keystone.authorized && !authorizeTenant(req.keystone.userData, group) && !isAdmin(req.keystone.userData)){
                return res.writePretty({"err":"Invalid token for tenant ["+group+"]"},401)
            }

            app.settings.bqAdm.deleteTopic(req.params.topicId,function(err,data){
               if(err){
                  var errMsg = err.msg || ""+err
                  return res.writePretty({"err":errMsg},err.code || 500)
                }
                return res.writePretty(undefined,204)

            })
        })
    })
    
    app.get(app.settings.basePath+"/topics/:topicId",function(req,res){
        var topic = req.params.topicId; 
        if(cache[topic] && cache[topic].data) {
          updateVisited(topic);
          res.setHeader("X-Cache-Time",cache[topic].lastRefresh)
          return res.writePretty(cache[topic].data,200);
        } else { 
          app.settings.bqAdm.getTopicData(topic,function(err,data){
              if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
              }
              initializeCache(topic);              
              return res.writePretty(data,200)
          })
        }
    })

    app.get(app.settings.basePath+"/topics/:topicId/consumers",function(req,res){
        var topic = req.params.topicId;  
        if(cache[topic] && cache[topic].data) {
          updateVisited(topic);
          res.setHeader("X-Cache-Time",cache[topic].lastRefresh)
          return res.writePretty(cache[topic].data.consumers,200);
        } else { 
          app.settings.bqAdm.getTopicData(topic,function(err,data){
             if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
              }
              initializeCache(topic);              
              return res.writePretty(data.consumers,200)
          });
        }
    })
    app.post(app.settings.basePath+"/topics/:topicId/consumers",function(req,res){
        if(!req.is("json")){    
            return res.writePretty({err:"Error parsing json"},400)
        }

        var group = req.body[app.settings.groupEntity]
        var topic = req.params.topicId
        var consumer = group+"-"+req.body.name

        if(!group){
            res.writePretty({"err":"The property ["+app.settings.groupEntity+"] must be set"},400)
        }

       
        if(req.keystone && req.keystone.authorized && !isAdmin(req.keystone.userData)){
            if(!authorizeTenant(req.keystone.userData, group))
                return res.writePretty({"err":"Invalid token for tenant ["+group+"]"},401)
                
            //Consumers can be only created if these belongs to the same tenant or the user has the admin role     
            if(topic.lastIndexOf(group,0) != 0)
                return res.writePretty({"err":"Tenant ["+group+"] can't create consumers on ["+topic+"]]"},401)    
        }

        if(!req.body.name){
            return res.writePretty({err:"Consumer should contains a name"},400)
        }
        app.settings.bqAdm.createConsumerGroup(topic,consumer,function(err){
            if(err){
              var errMsg = err.msg || ""+err
              return res.writePretty({"err":errMsg},err.code || 500)
            }
            app.settings.bqAdm.getConsumerData(topic,consumer,function(err,data){
                if(err){
                  var errMsg = err.msg || ""+err
                  return res.writePretty({"err":errMsg},err.code || 500)
                }
                return res.writePretty(data,201)
            })
        })
    })

    app.delete(app.settings.basePath+"/topics/:topicId/consumers/:consumerId",function(req,res){
        app.settings.bqAdm.getTopicGroup(req.params.topicId,function(err,group){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
        
            if(req.keystone && req.keystone.authorized && !authorizeTenant(req.keystone.userData, group) && !isAdmin(req.keystone.userData)){
                return res.writePretty({"err":"Invalid token for tenant ["+group+"]"},401)
            }

            app.settings.bqAdm.deleteConsumerGroup(req.params.topicId,req.params.consumerId,function(err,data){
                if(err){
                  var errMsg = err.msg || ""+err
                  return res.writePretty({"err":errMsg},err.code || 500)
                }
                return res.writePretty(undefined,204)
            })
        })
    })

    //Reset on put
    app.put(app.settings.basePath+"/topics/:topicId/consumers/:consumerId",function(req,res){
        app.settings.bqAdm.getTopicGroup(req.params.topicId,function(err,group){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
        
            if(req.keystone && req.keystone.authorized && !authorizeTenant(req.keystone.userData, group) && !isAdmin(req.keystone.userData)){
                return res.writePretty({"err":"Invalid token for tenant ["+group+"]"},401)
            }

            app.settings.bqAdm.resetConsumerGroup(req.params.topicId,req.params.consumerId,function(err,data){
                if(err){
                  var errMsg = err.msg || ""+err
                  return res.writePretty({"err":errMsg},err.code || 500)
                }
                return res.writePretty({"msg":"Consumer ["+req.params.consumerId+"] of topic ["+req.params.topicId+"] have been reset"},200)
            })
        })
    })
    app.get(app.settings.basePath+"/topics/:topicId/consumers/:consumerId",function(req,res){
        var topic = req.params.topicId;
        var consumer = req.params.consumerId;
        if(!req.headers["x-newest"] && cache[topic] && cache[topic].data) {
          updateVisited(topic);
          res.setHeader("X-Cache-Time",cache[topic].lastRefresh)
          var consumerData;
          var topicData = cache[topic].data;
          for(var i in topicData.consumers) {
            if(topicData.consumers[i].consumer_id == consumer) {
              consumerData = topicData.consumers[i];
              break;
            }
          }
          if(!consumerData) {
            res.writePretty({"err":"Consumer not found"},400);
          } else {
            res.writePretty(consumerData,200);
          }
        } else {
          app.settings.bqAdm.getConsumerData(topic,consumer,function(err,data){
              if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
              }
              initializeCache(topic);              
              return res.writePretty(data,200)
          })
        }
    })

}

/**
 * Cache it's a simple object indexed by topic id,
 * for each id will be abailable
 * - Data: the data from redis and zookeeper
 * - lastRefresh: the time from the last refresh
 * - lastVisit: the time form the last read
 */
var loadCacheRefresher = function(cache, app) {
  var refreshInterval = app.settings.cacheRefreshInterval || 30000;
  var cacheWhileVisitTime = app.settings.cacheWhileVisitTime || 300000;
  function updateTopicCache(topic) {
    app.settings.bqAdm.getTopicData(topic,function(err,data) {
        if(err) {
          //On error the cache item will be removed
          delete cache[topic];
        } else {
          cache[topic].data = data;
          cache[topic].lastRefresh = new Date().getTime();
        }
    });

  }
  function refreshCacheCron() {
    log.inf("Refreshing topic cache");
    var keys = Object.keys(cache);
    for(var i in keys) {
      var key = keys[i];
      if(cache[key].lastVisit < (new Date().getTime() - cacheWhileVisitTime)) {
        delete cache[key];
      } else {
        updateTopicCache(key);
      }
    }
    if(app.running) {
      setTimeout(refreshCacheCron, refreshInterval);
    } else {
      cache = {};
    }
  }

  refreshCacheCron(); 
} 

var authFilter = function(config){

    return function(req,res,next){
        //All post should be authenticated
        if((req.method.toUpperCase() === "POST" || req.method.toUpperCase() === "DELETE") && !req.keystone.authorized){
            res.writePretty({"err":"All post to admin api should be authenticated using a valid X-Auth-Token header"},401)
        }else{
            next()
        }
    }
}
var writeFilter = function(){
    return function(req,res,next){
        res.writePretty = function(obj,statusCode){
            if(req.accepts("json")){
                res.json(obj,statusCode)
            }else if(req.accepts("text/plain")){
                res.send(YAML.stringify(obj),statusCode)
            }else{
                //Default
                res.json(obj,statusCode)
            }
        }
        next()
    }
}

exports.startup = function(config){
    log.setLevel(config.logLevel || "info")
    //Default 5 days
    var maxTtl = config.maxTtl || 5*24*60*60 
    var app = express.createServer()
        if(config.loggerConf){
        log.inf("Using express logger")
        app.use(express.logger(config.loggerConf));
    }
    var topicDataCache = {};
    
    app.use(writeFilter())
    app.enable("jsonp callback")
        
    app.use(express.bodyParser());

    if(config.keystoneConfig){
        app.use(keystoneMiddlware.auth(config.keystoneConfig))
        app.use(authFilter())
        app.set("adminRoleId",config.admConfig.adminRoleId || -1)
    }

    app.use(app.router); 

    app.set("basePath",config.basePath || "")
    app.set("maxTtl",maxTtl)
    app.set("bqAdm",bqAdm.createClustersAdminClient(config.admConfig))
    app.set("topicDataCache", topicDataCache);
    app.set("cacheRefreshInterval", config.cacheRefreshInterval || 30000);
    app.set("cacheWhileVisitTime", config.cacheWhileVisitTime || 300000);

    var groupEntity = config.groupEntity || "tenantId"
    app.set("groupEntity",groupEntity )
    loadApp(app)
    app.running = true;
    loadCacheRefresher(topicDataCache, app); 
    app.listen(config.port)
    this.app = app
    return this
}

exports.shutdown = function(){
    if(this.app.settings.bqAdm)
        this.app.settings.bqAdm.shutdown()
    this.app.close()
    this.app.running = false;
}
