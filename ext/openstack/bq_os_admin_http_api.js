var express = require('express'),
    log = require('node-logging'),
    bqAdm = require('../../lib/bq_clusters_adm.js'),
    keystoneMiddlware = require("../../ext/openstack/keystone_middleware.js")

var loadApp = function(app){
    var admClient = app.settings.bqAdm

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

    app.get(app.settings.basePath+"/clusters",function(req,res){
        admClient.listClusters(function(err,clusters){
            if(err)
                return res.json({"err":err},500)
            return res.json(clusters,200)
        })
    })

    app.post(app.settings.basePath+"/clusters",function(req,res){
        if(!req.is("json")){    
            return res.json({err:"Error parsing json"},400)
        }
        admClient.createBigQueueCluster(req.body,function(err){
            if(err)
                return res.json({"err":err},500)
            return res.json({"cluster":req.body.name},201)
        })
    })

    app.post(app.settings.basePath+"/clusters/:cluster/nodes",function(req,res){
        if(!req.is("json")){    
            return res.json({err:"Error parsing json"},400)
        }
        if(!req.body.name){
            return res.json({err:"Node should contains name"},400)
        }
        admClient.addNodeToCluster(req.params.cluster,req.body,function(err){
            if(err)
                return res.json({"err":err},500)
            return res.json({"cluster":req.body.name},201)
        })
    })

    app.post(app.settings.basePath+"/clusters/:cluster/journals",function(req,res){
        if(!req.is("json")){    
            return res.json({err:"Error parsing json"},400)
        }
        if(!req.body.name){
            return res.json({err:"Node should contains name"},400)
        }
        admClient.addJournalToCluster(req.params.cluster,req.body,function(err){
            if(err)
                return res.json({"err":err},500)
            return res.json({"cluster":req.body.name},201)
        })
    })

    app.post(app.settings.basePath+"/clusters/:cluster/endpoints",function(req,res){
        if(!req.is("json")){    
            return res.json({err:"Error parsing json"},400)
        }
        if(!req.body.name){
            return res.json({err:"Node should contains name"},400)
        }
        admClient.addEntrypointToCluster(req.params.cluster,req.body,function(err){
            if(err)
                return res.json({"err":err},500)
            return res.json({"cluster":req.body.name},201)
        })
    })


    app.put(app.settings.basePath+"/clusters/:cluster/nodes/:node",function(req,res){
        if(!req.is("json")){    
            return res.json({err:"Error parsing json"},400)
        }
        var node = req.body
        node["name"] = req.params.node 
        admClient.updateNodeData(req.params.cluster,node,function(err){
            if(err)
                return res.json({"err":err},500)
            return res.json({"cluster":req.body.name},200)
        })
    })

    app.get(app.settings.basePath+"/clusters/:cluster",function(req,res){
        admClient.getClusterData(req.params.cluster,function(err,data){
            if(err)
                return res.json({"err":err},500)
            return res.json(data,200)
        })
    })

    app.get(app.settings.basePath+"/topics",function(req,res){
        var group = req.query[app.settings.groupEntity]
        if(!group){
            return res.json({err:"The parameter ["+app.settings.groupEntity+"] must be set"},400)
        }
        admClient.getGroupTopics(group,function(err,data){
           if(err)
                return res.json({"err":err},500)
            return res.json(data,200)
        })
    })

    app.post(app.settings.basePath+"/topics",function(req,res){
        if(!req.is("json")){    
            return res.json({err:"Error parsing json"},400)
        }
        var group = req.body[app.settings.groupEntity]

        if(!group){
            return res.json({"err":"The property ["+app.settings.groupEntity+"] must be set"},400)
        }

        if(req.keystone && req.keystone.authorized && !authorizeTenant(req.keystone.userData, group) && !isAdmin(req.keystone.userData)){
            return res.json({"err":"Invalid token for tenant ["+group+"]"},401)
        }
        if(!req.body.name){
            return res.json({err:"Topics should contains a name"},400)
        }
        var topic = group+"-"+req.body.name
        var ttl = req.body.ttl
        if(ttl && ttl > app.settings.maxTtl){
            return res.json({"err":"Max ttl exceeded, max ttl possible: "+app.settings.maxTtl},406)
        }
        admClient.createTopic({"name":topic,"group":group,"ttl":ttl},req.body.cluster,function(err){
            if(err){
              return res.json({"err":err},500)
            }else{
                admClient.getTopicData(topic,function(err,data){
                    if(err){
                      return res.json({"err":err},500)
                    }
                    return res.json(data,201)
                })
            }

        })
    })
    
    app.get(app.settings.basePath+"/topics/:topicId",function(req,res){
        admClient.getTopicData(req.params.topicId,function(err,data){
            if(err){
              return res.json({"err":err},500)
            }
            return res.json(data,200)
        })
    })

    app.get(app.settings.basePath+"/topics/:topicId/consumers",function(req,res){
        admClient.getTopicData(req.params.topicId,function(err,data){
           if(err){
              return res.json({"err":err},500)
            }
            return res.json(data.consumers,200)
        })
    })
    app.post(app.settings.basePath+"/topics/:topicId/consumers",function(req,res){
        if(!req.is("json")){    
            return res.json({err:"Error parsing json"},400)
        }

        var group = req.body[app.settings.groupEntity]
        var topic = req.params.topicId
        var consumer = group+"-"+req.body.name

        if(!group){
            res.json({"err":"The property ["+app.settings.groupEntity+"] must be set"},400)
        }

       
        if(req.keystone && req.keystone.authorized && !isAdmin(req.keystone.userData)){
            if(!authorizeTenant(req.keystone.userData, group))
                return res.json({"err":"Invalid token for tenant ["+group+"]"},401)
                
            //Consumers can be only created if these belongs to the same tenant or the user has the admin role     
            if(topic.lastIndexOf(group,0) != 0)
                return res.json({"err":"Tenant ["+group+"] can't create consumers on ["+topic+"]]"},401)    
        }

        if(!req.body.name){
            return res.json({err:"Consumer should contains a name"},400)
        }
        admClient.createConsumer(topic,consumer,function(err){
            if(err)
              return res.json({"err":err},500)
            admClient.getConsumerData(topic,consumer,function(err,data){
                if(err){
                  return res.json({"err":err},500)
                }
                return res.json(data,201)
            })
        })
    })

    app.get(app.settings.basePath+"/topics/:topicId/consumers/:consumerId",function(req,res){
        admClient.getConsumerData(req.params.topicId,req.params.consumerId,function(err,data){
            if(err){
              return res.json({"err":err},500)
            }
            return res.json(data,200)
        })
    })

}

var authFilter = function(config){

    return function(req,res,next){
        //All post should be authenticated
        if(req.method === "POST" && !req.keystone.authorized){
            res.json({"err":"All post to admin api should be authenticated using X-Auth-Token header"},401)
        }else{
            next()
        }
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
        
    app.use(express.bodyParser());

    if(config.keystoneConfig){
        app.use(keystoneMiddlware.auth(config.keystoneConfig))
        app.use(authFilter())
        app.set("adminRoleId",config.admConfig.adminRoleId || -1)
    }
    app.use(app.router); 

    app.set("basePath",config.basePath || "")
    app.set("bqAdm", bqAdm.createClustersAdminClient(config.admConfig))
    app.set("maxTtl",maxTtl)

    var groupEntity = config.groupEntity || "tenantId"
    app.set("groupEntity",groupEntity )

    loadApp(app) 
    app.listen(config.port)
    this.app = app
    return this
}

exports.shutdown = function(){
    this.app.settings.bqAdm.shutdown()
    this.app.close()
}
