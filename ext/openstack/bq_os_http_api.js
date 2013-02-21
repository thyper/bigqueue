var express = require('express'),
    log = require('node-logging')
var maxBody = "64kb"
var bqClient

var valid_element_regex=/^(\w|[0-9]){2,50}$/

var loadApp = function(app){
    app.get("/ping",function(req,res){
      res.end("pong")
    })

    app.post(app.settings.basePath+"/messages",function(req,res){
        if(!req.is("json")){
            return res.json({err:"Message should be json"},400)
        }
        var topics = req.body.topics
        if(!topics){
            return res.json({err:"should be declared the 'topics' property"},400)
        }
        delete req.body["topics"]
        var message
        try{
            message = req.body
            Object.keys(message).forEach(function(val){
                if(message[val] instanceof Object){
                    message[val] = JSON.stringify(message[val])
                }
            })
        }catch(e){
            return res.json({err:"Error parsing json ["+e+"]"},400)
        }
 
        var errors = []
        var datas = []
        var executed = 0
        for(var i in topics){
            app.settings.bqClient.postMessage(topics[i],message,function(err,data){
               if(err){
                    errors.push(err.msg)
                }
                if(data){
                    datas.push(data)
                }
                executed++
                if(executed == topics.length){
                    if(errors.length>0){
                        return res.json({err:"An error ocurrs posting the messages","errors":errors},500) }else{
                        return res.json(datas,201)
                    }
                }
            })
        }
    })

    app.get(app.settings.basePath+"/topics/:topic/consumers/:consumer/messages",function(req,res){
        var timer = log.startTimer()
        try{
            var topic = req.params.topic
            var consumer = req.params.consumer
            timer("Starting message get")
            app.settings.bqClient.getMessage(topic,consumer,req.query.visibilityWindow,function(err,data){
                timer("Message getted")
                if(err){
                    if(typeof(err) == "string")
                        res.json({"err":""+err},400)
                    else
                        res.json(err,400)
                }else{
                    if(data && data.id){
                        Object.keys(data).forEach(function(val){
                            if(data[val].match(/\{.*\}/) || data[val].match(/\[.*\]/)){
                                var orig = data[val]
                                try{
                                    data[val] = JSON.parse(data[val])
                                }catch(e){
                                    //On error do nothing
                                }
                            }
                        })
                        timer("Getted message througt web-api")
                        res.json(data,200)
                    }else{
                        timer("Getted void message throught web-api")
                        res.json({},204)
                    }
                }
            })
        }catch(e){
            log.err("Error getting message ["+e+"]")
            res.json({err:"Error processing request ["+e+"]"},500)
        }
    })

    app.delete(app.settings.basePath+"/topics/:topic/consumers/:consumerName/messages/:recipientCallback",function(req,res){
        try{
            var topic = req.params.topic
            var consumer = req.params.consumerName
            app.settings.bqClient.ackMessage(topic,consumer,req.params.recipientCallback,function(err){
                if(err){
                    var errMsg = err.msg || ""+err
                    res.json({err:errMsg},200)
                }else{
                    res.json({},204)
                }
            })
        }catch(e){
            log.err("Error deleting message ["+e+"]")
            res.json({err:"Error processing request ["+e+"]"},500)
        }

    })
}

exports.startup = function(config){
    log.setLevel(config.logLevel || "info")
     
    var app = express.createServer()
        if(config.loggerConf){
        log.inf("Using express logger")
        app.use(express.logger(config.loggerConf));
    }
    app.set("bqClient",config.bqClientCreateFunction(config.bqConfig))
    app.set("basePath",config.basePath || "")

    app.use(express.limit(maxBody));
    app.use(express.bodyParser());
    app.use(express.methodOverride());

    app.use(app.router); 

    loadApp(app) 

    app.listen(config.port)
    console.log("http api running on ["+config.port+"]")
    this.app = app
}

exports.shutdown = function(){
    this.app.close()
}
