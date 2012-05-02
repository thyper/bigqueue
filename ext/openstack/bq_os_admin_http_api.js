var express = require('express'),
    log = require('node-logging')
    bqAdm = require('../../lib/bq_clusters_adm.js')

var loadApp = function(app){
    var admClient = app.settings.bqAdm
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

    app.post(app.settings.basePath+"/topics/:tenantId",function(req,res){
        if(!req.is("json")){    
            return res.json({err:"Error parsing json"},400)
        }
        if(!req.body.name){
            return res.json({err:"Topics should contains a name"},400)
        }
        var topic = req.params.tenantId+"-"+req.body.name
        admClient.createTopic(topic,req.body.cluster,function(err){
            if(err)
              return res.json({"err":err},500)
            return res.json({"name":req.body.name},201) 
        })
    })
    
}

exports.startup = function(config){
    log.setLevel(config.logLevel || "info")
     
    var app = express.createServer()
        if(config.loggerConf){
        log.inf("Using express logger")
        app.use(express.logger(config.loggerConf));
    }
        
    app.use(express.bodyParser());

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
