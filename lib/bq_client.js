var redis = require('redis'),
    fs = require('fs'),
    should = require('should'),
    events = require("events"),
    log = require("node-logging"),
    bqutils = require("../lib/bq_client_utils.js")
var TOTAL_SCRIPTS = 10
var redisScripts = {};
/**
 *   scripts load
**/
fs.readFile(__dirname+'/../scripts/getMessage.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["getMessage"] = strFile
})
fs.readFile(__dirname+'/../scripts/postMessage.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["postMessage"] = strFile
})
fs.readFile(__dirname+'/../scripts/createConsumer.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["createConsumerGroup"] = strFile
})
fs.readFile(__dirname+'/../scripts/createTopic.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["createTopic"] = strFile
})
fs.readFile(__dirname+'/../scripts/ack.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["ackMessage"] = strFile
})
fs.readFile(__dirname+'/../scripts/fail.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["failMessage"] = strFile
})
fs.readFile(__dirname+'/../scripts/consumerStats.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["consumerStats"] = strFile
})
fs.readFile(__dirname+'/../scripts/deleteTopic.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["deleteTopic"] = strFile
})
fs.readFile(__dirname+'/../scripts/deleteConsumer.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["deleteConsumer"] = strFile
})
fs.readFile(__dirname+'/../scripts/resetConsumerGroup.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["resetConsumerGroup"] = strFile
})


/**
 *   finish scripts load
**/



function BigQueueClient(rConf){
    events.EventEmitter.call( this );
    this.redisConf = rConf
    this.redisLoaded = false
    this.shutdowned = false
    var self = this;
    self.emitReadyWhenAllLoaded()
}

BigQueueClient.prototype = new events.EventEmitter();

BigQueueClient.prototype.emitReadyWhenAllLoaded = function(){
    var self = this;
    var scriptsLoaded = (Object.keys(redisScripts).length == TOTAL_SCRIPTS)
    if(scriptsLoaded && this.redisLoaded){
        /**
         * If redis is up but is loading will response but redis is not ready 
         * so we will check every sec
         */
        var checkRedisPing = function(){
            if(!self.shutdowned){
                self.redisClient.execute("PING",function(err,data){
                    if(!err && data == "PONG"){
                        self.emit("ready")
                    }else{
                        self.emit("error",err)
                        setTimeout(checkRedisPing,1000)
                    }
                })
            }
        }
        checkRedisPing()
    }else{
        setTimeout(function(){
            self.emitReadyWhenAllLoaded()
        },1);
    }
}

BigQueueClient.prototype.connect = function(){
    var self = this;
    try{
        this.redisClient = redis.createClient(this.redisConf.port,this.redisConf.host,this.redisConf.options)
        this.redisClient.execute = function() {
          var args = [];
          for (var key in arguments) {
            args.push(arguments[key]);
          }
          var command = args.shift();
          var callback = args.pop();
          return self.redisClient.send_command(command, args, callback);
        }

        this.redisClient.on("error",function(err){
            log.err("Error connecting to ["+JSON.stringify(self.redisConf)+"]",err)
            err["redisConf"] = self.redisConf
            process.nextTick(function(){
                try{
                    self.emit("error",err)
                  }catch(e){
                }
            })
        })
        this.redisClient.on("connect",function(){
            self.emit("connect",self.redisConf)
        })
        this.redisClient.on("ready",function(){
            log.dbg("Connected to redis ["+log.pretty(self.redisConf)+"]")
            self.redisLoaded = true;
        })
    }catch(e){
        this.emit("error",e)
    }
}

BigQueueClient.prototype.createTopic = function(topic, callback){
    var topic = arguments[0]
    var ttl = -1
    var callback
    if(arguments.length == 3){
        if(ttl != undefined){
            ttl = arguments[1]
        }
        callback = arguments[2]
    }else{
        callback = arguments[1]
    }
    this.redisClient.execute("EVAL",redisScripts["createTopic"],0,topic,ttl,function(err,data){
       if(err){
            log.err("Error creating topic ["+err+"]")
            err = {"msg":err}
        }
        callCallback(callback,err)
    })
}

BigQueueClient.prototype.deleteTopic = function(topic,callback){
    this.redisClient.execute("EVAL",redisScripts["deleteTopic"],0,topic,function(err,data){
        if(err){
            log.err("Error deleting topic ["+err+"]")
            err = {"msg":err}
        }
        callCallback(callback,err)
    })
}

BigQueueClient.prototype.createConsumerGroup = function(topic, consumerGroup,callback){
    this.redisClient.execute("EVAL",redisScripts["createConsumerGroup"],0,topic,consumerGroup,function(err,data){
        if(err){
            log.err("Error creating group ["+consumerGroup+"] for topic ["+topic+"], error: "+err)
            err = {"msg":err}
        }
        callCallback(callback,err)
    })
}

BigQueueClient.prototype.deleteConsumerGroup = function(topic,consumerGroup,callback){
    this.redisClient.execute("EVAL",redisScripts["deleteConsumer"],0,topic,consumerGroup,function(err,data){
        if(err){
            log.err("Error deleting group ["+consumerGroup+"] for topic ["+topic+"], error: "+err)
            err = {"msg":err}
        }
        callCallback(callback,err)
    })

}

BigQueueClient.prototype.resetConsumerGroup = function(topic, consumerGroup,callback){
    this.redisClient.execute("EVAL",redisScripts["resetConsumerGroup"],0,topic,consumerGroup,function(err,data){
        if(err){
            log.err("Error reseting group ["+consumerGroup+"] for topic ["+topic+"], error: "+err)
            err = {"msg":err}
        }
        callCallback(callback,err)
    })
}

BigQueueClient.prototype.postMessage = function(topic, message,callback){
    if(message.msg == undefined){
        callCallback(callback,{"msg":"The message should have the 'msg' property"})
        return
    }
    var timer = log.startTimer()
    var self = this
    this.redisClient.execute("EVAL",redisScripts["postMessage"],0,topic,JSON.stringify(message),function(err,data){
        timer("Post message to redis "+self.redisConf.host+"-"+self.redisConf.port)
        if(err){
            log.err("Error posting message ["+log.pretty(message)+"] ["+self.redisConf.host+"] [topic: "+topic+"], error: "+err)
            err = {"msg":""+err}
            callCallback(callback,err)
        }else{
            if(callback)
                callback(undefined,{"id":data,"topic":topic})
        }
    })  
}

BigQueueClient.prototype.getMessage = function(topic,consumer,visibilityWindow,callback){
    visibilityWindow = visibilityWindow || -1
    var tms = Math.floor(new Date().getTime() / 1000)
    var timer = log.startTimer()
    var self = this
    this.redisClient.execute("EVAL",redisScripts["getMessage"],0,tms,topic,consumer,visibilityWindow,function(err,data){
        timer("Get Message from redis "+self.redisConf.port)
        if(err){
            log.err("Error getting messages of topic ["+topic+"] for consumer ["+consumer+"], error: "+err)
            err = {"msg":""+err}
            callCallback(callback,err)
        }else{
            if(callback){
                var data = bqutils.redisListToObj(data)
                data.recipientCallback = data.id
                callback(undefined,data)
            }
        }
    })
}

BigQueueClient.prototype.ackMessage = function(topic,consumerGroup,id,callback){
    this.redisClient.execute("EVAL",redisScripts["ackMessage"],0,topic,consumerGroup,id,function(err,data){
        if(err){
            log.err("Error doing ack for message ["+id+"] in topic ["+topic+"] for consumer ["+consumerGroup+"], error: "+log.pretty(err))
            err = {"msg":""+err}
            callCallback(callback,err)
        }else{
            if(data<=0)
                callCallback(callback,{"msg":"no message was acked"})
            else
                callCallback(callback)
        }
        
    })
}

BigQueueClient.prototype.failMessage = function(topic,consumerGroup,id,callback){
    this.redisClient.execute("EVAL",redisScripts["failMessage"],0,topic,consumerGroup,id,function(err,data){
       if(err)
        err = {"msg":""+err}
       callCallback(callback,err)
    })
}

BigQueueClient.prototype.getTopicTtl = function(topic,callback){
    this.redisClient.execute("GET","topics:"+topic+":ttl",function(err,data){
        if(err){
            err = {"msg":""+err}
            callback(err)
        }else{
            callback(undefined,data)
        }
    })
}

BigQueueClient.prototype.listTopics = function(callback){
    this.redisClient.execute("SMEMBERS","topics",function(err,data){
        if(err)
            log.err("Error listing topics ["+log.pretty(err)+"]")
        callCallback(callback,data)
    })
    
}

BigQueueClient.prototype.getConsumerGroups = function(topic,callback){
    var self = this
    this.redisClient.execute("SISMEMBER","topics",topic,function(err,data){
        if(data == 0){
            callCallback(callback,{"msg":"Error topic not found","code":404})
            return
        }
        self.redisClient.execute("SMEMBERS","topics:"+topic+":consumers",function(err,data){
            if(err){
                log.err("Error getting consumer groups for topic ["+topic+"], error: "+err)
                callback(err)
            }
            if(callback)
                callback(undefined,data)
        })
    })
}

BigQueueClient.prototype.getConsumerStats = function(topic,consumer,callback){
    var self = this
    this.redisClient.execute("EVAL",redisScripts["consumerStats"],0,topic,consumer,function(err,data){
        if(err){
            log.err("Error getting consumer stats for consumer ["+consumer+"] of topic ["+topic+"], error: "+err)
            err = {"msg":""+err}
            callCallback(callback,err)
        }else{
            callback(undefined,bqutils.redisListToObj(data))
        }
    })
}

BigQueueClient.prototype.shutdown = function(){
    this.shutdowned = true 
    try{
        this.redisClient.quit()
        this.redisClient.closing = true
    }catch(e){}
}

BigQueueClient.prototype.getHeads = function(callback){
    var self = this
    this.listTopics(function(data){
        if(!data){
            callback({"msg":"Error getting topics"},undefined)
            return
        }
        if(data.length == 0)
            callback(undefined,[])
        var dataCount = 0
        var dataRet = {}
        for(var i in data){
            self.getHead(data[i],function(err,head){
                if(err){
                    err = {"msg":""+err}
                    callback(err,undefined)
                    return
                }
                dataRet[head.topic]=head.head
                dataCount++
                if(dataCount == data.length){
                    callback(undefined,dataRet)
                }
            })
        }
    })
}

BigQueueClient.prototype.getNodeStats = function(callback) {
  var self = this;
  self.listTopics(function(topics) {
    var resp = {
      sample_date: new Date().getTime(),
      topic_stats:{}
    };
    var totalResponses = topics.length;
    var responses = 0;
    function checkIfCallback() {
      if(responses == totalResponses) {
        callback(undefined, resp);
      }
    }
    if(topics.length == 0) {
      checkIfCallback();
    }
    topics.forEach(function(topic) {
      responses++;
      resp.topic_stats[topic] = {};
      self.getConsumerGroups(topic, function(err, consumers) {
        if(err) {
          return callback(err);
        }
        if(consumers.length == 0) {
          checkIfCallback();
        } 
        totalResponses+=consumers.length;
        consumers.forEach(function(consumer) {
           self.getConsumerStats(topic, consumer, function(err, stats) {
             if(err) 
               callback(err);
             resp.topic_stats[topic][consumer] = stats;
             responses++;
             checkIfCallback();
          });
        });
      });
    });
  });
} 

BigQueueClient.prototype.getHead = function(topic,callback){
    this.redisClient.execute("GET","topics:"+topic+":head",function(err,data){
        if(err){
            err = {"msg":""+err}
            callback(err,undefined)
        }
        if(!data){
            data = -1
        }
        callback(undefined,{"topic":topic,"head":data})
    })
}

function callCallback(){
    if(arguments.length <=0 )
        return;
    var callback = arguments[0]
    var check = arguments[1]
    if(callback){
        if(check){
            callback(check)
        }else{
            callback()
        }
    }

}
exports.bqClient = BigQueueClient
exports.createClient = function(redisConf){
    if(!redisConf.options)
        redisConf.options = {}
    redisConf.options["return_buffers"] = false
    var cli = new BigQueueClient(redisConf)
    cli.connect()
    return cli;
}

