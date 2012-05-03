var express = require('express'),
    log = require('node-logging'),
    bqAdm = require('../../lib/bq_clusters_adm.js'),
    keystoneMiddlware = require("../../ext/openstack/keystone_middleware.js")

var loadApp = function(app){
    var admClient = app.settings.bqAdm

    var authorizeTenant = function(userData,tenant){
        var authorized = false
        try{
            var tenants = userData.access.token.tenants
            tenants.forEach(function(val){
                if(val.name == tenant){
                    authorized = true
                    return
                }
            })
        }catch(e){
            //Property doesn't exist
        }
        return authorized
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

    app.get(app.settings.basePath+"/clusters/:cluster",function(req,res){
        admClient.getClusterData(req.params.cluster,function(err,data){
            if(err)
                return res.json({"err":err},500)
            return res.json(data,200)
        })
    })

    app.get(app.settings.basePath+"/:tenantId/topics",function(req,res){
        admClient.getGroupTopics(req.params.tenantId,function(err,data){
           if(err)
                return res.json({"err":err},500)
            for(var i in data){
                var pos = data[i].indexOf("-")
                if(pos != -1){
                    data[i] = data[i].substr(pos+1)
                }
            }
            return res.json(data,200)
        })
    })

    app.post(app.settings.basePath+"/:tenantId/topics",function(req,res){
        if(!req.is("json")){    
            return res.json({err:"Error parsing json"},400)
        }
        var tenant = req.params.tenantId
        if(req.keystone && req.keystone.authorized && !authorizeTenant(req.keystone.userData, tenant)){
            return res.json({"err":"Invalid token for tenant ["+req.params.tenantId+"]"},401)
        }
        if(!req.body.name){
            return res.json({err:"Topics should contains a name"},400)
        }
        var topic = req.params.tenantId+"-"+req.body.name
        admClient.createTopic({"name":topic,"group":tenant},req.body.cluster,function(err){
            if(err)
              return res.json({"err":err},500)
            return res.json({"name":req.body.name},201) 
        })
    })
    
    app.get(app.settings.basePath+"/:tenantId/topics/:topic",function(req,res){
        var topic = req.params.tenantId+"-"+req.params.topic
        admClient.getTopicData(topic,function(err,data){
            if(err){
              return res.json({"err":err},500)
            }
            return res.json(data,200)
        })
    })

    app.post(app.settings.basePath+"/:tenantIdTopic/topics/:topicId/consumers/:tenantIdConsumer",function(req,res){
        if(!req.is("json")){    
            return res.json({err:"Error parsing json"},400)
        }
        
        var tenant = req.params.tenantIdConsumer
        if(req.keystone && req.keystone.authorized && !authorizeTenant(req.keystone.userData, tenant)){
            return res.json({"err":"Invalid token for tenant ["+req.params.tenantId+"]"},401)
        }

        if(!req.body.name){
            return res.json({err:"Consumer should contains a name"},400)
        }
        var topic = req.params.tenantIdTopic+"-"+req.params.topicId
        var consumer = req.params.tenantIdConsumer+"-"+req.body.name
        admClient.createConsumer(topic,consumer,function(err){
            if(err)
              return res.json({"err":err},500)
            return res.json({"name":req.body.name},201) 
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
     
    var app = express.createServer()
        if(config.loggerConf){
        log.inf("Using express logger")
        app.use(express.logger(config.loggerConf));
    }
        
    app.use(express.bodyParser());

    if(config.keystoneConfig){
        app.use(keystoneMiddlware.auth(config.keystoneConfig))
        app.use(authFilter())
    }
    app.use(app.router); 

    app.set("basePath",config.basePath)
    app.set("bqAdm", bqAdm.createClustersAdminClient(config.admConfig))

    loadApp(app) 
    app.listen(config.port)
    this.app = app

    return this
}

exports.shutdown = function(){
    this.app.settings.bqAdm.shutdown()
    this.app.close()
}
