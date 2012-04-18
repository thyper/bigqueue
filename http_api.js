var express = require('express'),
    log = require('node-logging')
var maxBody = "64kb"
var bqClient

var valid_element_regex=/^(\w|[0-9]){2,50}$/

var loadApp = function(app){
    app.get("/ping",function(req,res){
      res.end("pong")
    })

    app.get("/topics",function(req,res){
        try{
            bqClient.listTopics(function(data){
                res.json(data,200)
            })
        }catch(e){
            log.err("Error getting topics ["+e+"]",true)
            res.json({err:"Error processing request ["+e+"]"},500)
        }

    })

    app.post("/topics",function(req,res){
        if(!req.is("json")){    
            return res.json({err:"Error parsing json"},400)
        }
        var topic = req.body
        try{
            if(!topic.name.match(valid_element_regex)){
                return res.json({err:"Topic should be an string without special chars between 2 and 50 chars"},400)
            }
            bqClient.createTopic(topic.name,function(err){
                if(err){
                    log.err("Error creating topic ["+err+"]")
                    return res.json({err:""+err},409)
                }else{
                    return res.json({name:topic.name},201)
                }
            })
        }catch(e){
            log.err("Error creating topic ["+e+"]",true)
            return res.json({err:"Error processing request ["+e+"]"},500)
        }
    })

    app.get("/topics/:topic/consumerGroups",function(req,res){
        try{
            bqClient.getConsumerGroups(req.params.topic,function(err,data){
                if(err){
                    log.err("Error creating consumer group ["+log.pretty(req.params)+"] ["+err+"]")
                    res.json({err:""+err},400)
                }else{
                    res.json(data,200)
                }
            })
        }catch(e){
            log.err("Error creating consumer group ["+e+"]",true)
            res.json({err:"Error processing request ["+e+"]"},500)
        }

    })

    app.post("/topics/:topic/consumerGroups",function(req,res){
            if(!req.is("json")){
                return res.json({err:"Content should be json"},400)
            }
            var consumer = req.body
            var topic = req.params.topic
            if(!consumer.name.match(valid_element_regex)){
              return res.json({err:"Consumer group should be an string without special chars between 2 and 50 chars"})
            }

            try{
                bqClient.createConsumerGroup(topic,consumer.name,function(err){
                    if(err)
                        return res.json({err:""+err},409)
                    return res.json({name:consumer.name},201)
                })
            }catch(e){
                return res.json({err:"Error processing request ["+e+"]"},500)
            }


    })

    app.post("/topics/:topic/messages",function(req,res){
        var timer = log.startTimer()
        if(!req.is("json")){
            return res.json({err:"Message should be json"},400)
        }
        var message
        try{
            message = req.body
            Object.keys(message).forEach(function(val){
                if(message[val] instanceof Object){
                    message[val] = JSON.stringify(message[val])
                }
            })
            timer("[REST-API] Json keys stringified")
        }catch(e){
            return res.json({err:"Error parsing json ["+e+"]"},400)
        }
        try{
            timer("[REST-API] Starting data save")
            bqClient.postMessage(req.params.topic,message,function(err,data){
                timer("[REST-API]Posted message receive")
                if(err)
                   return res.json({err:""+err},400)
                return res.json(data,201)
                timer("[REST-API] Post user responsed")
            })
        }catch(e){
            log.err("Error posting message ["+e+"]",true)
            return res.json({err:"Error processing request ["+e+"]"},500)
        }
    })

    app.get("/topics/:topic/consumerGroups/:consumer/messages",function(req,res){
        var timer = log.startTimer()
        try{
            bqClient.getMessage(req.params.topic,req.params.consumer,req.query.visibilityWindow,function(err,data){
                if(err){
                    res.json({err:""+err},400)
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
            log.err("Error getting message ["+e+"]",true)
            res.json({err:"Error processing request ["+e+"]"},500)
        }
    })

    app.delete("/topics/:topic/consumerGroups/:consumer/messages/:recipientCallback",function(req,res){
        try{
            bqClient.ackMessage(req.params.topic,req.params.consumer,req.params.recipientCallback,function(err){
                if(err){
                    res.json({err:""+err},404)
                }else{
                    res.json({},204)
                }
            })
        }catch(e){
            log.err("Error deleting message ["+e+"]",true)
            res.json({err:"Error processing request ["+e+"]"},500)
        }

    })

    app.get("/topics/:topic/consumerGroups/:consumer/stats",function(req,res){
        try{
            bqClient.getConsumerStats(req.params.topic,req.params.consumer,function(err,data){
                if(err){
                    res.json({err:""+err},400)
                }else{
                    res.json(data,200)
                }
            })
        }catch(e){
            log.err("Error getting the stats for consumer")
            res.json({err:"Error processing request ["+e+"]"},500)
        }
    })

    app.get("/topics/:topic/stats",function(req,res){
        try{
            bqClient.getConsumerGroups(req.params.topic,function(err,consumers){
                if(err){
                    log.err("Error getting consumer groups for topic, error: "+err)
                    res.json({err:""+err},400)
                    return
                }
                var total=consumers.length
                var executed=0
                var data=[]
                if(total== 0){
                    res.json([],200)
                    return
                }
                consumers.forEach(function(consumer){
                    bqClient.getConsumerStats(req.params.topic,consumer,function(err,stats){
                        if(err){
                            res.json({err:""+err},400)
                        }else{
                            var d = {"consumer":consumer}
                            d.stats=stats
                            data.push(d)
                            executed++
                        }
                        //If an error ocurs the executed never will be equals than total
                        if(executed>=total){
                            res.json(data,200)
                        }
                    })
                })
            })
        }catch(e){
            log.err("Error getting the stats for topic")
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
    app.use(express.limit(maxBody));
    app.use(express.bodyParser());
    app.use(express.methodOverride());

    app.use(app.router); 

    loadApp(app) 

    app.listen(config.port)
    bqClient = config.bqClientCreateFunction(config.bqConfig)
    console.log("http api running on ["+config.port+"]")
}

exports.shutdown = function(){
}
