var redis = require('redis'),
    bqutils = require('../lib/bq_client_utils.js'),
    fs = require('fs'),
    should = require('should'),
    events = require("events"),
    log = require("node-logging")

/**
 *   scripts load
**/
var TOTAL_SCRIPTS = 2
var redisScripts= {}
fs.readFile(__dirname+'/../scripts/journalWrite.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["journalWrite"] = strFile
})
fs.readFile(__dirname+'/../scripts/journalRetrieve.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["journalRetrieve"] = strFile
})


function BigQueueJournalClient(conf){
    this.redisConf = conf
    this.redisLoaded = false
    this.shutdowned = false
    var self = this;
    self.emitReadyWhenAllLoaded()

}


BigQueueJournalClient.prototype = new events.EventEmitter();

BigQueueJournalClient.prototype.emitReadyWhenAllLoaded = function(){
    var self = this;
    var scriptsLoaded = (Object.keys(redisScripts).length == TOTAL_SCRIPTS)
    if(scriptsLoaded && this.redisLoaded){
        var checkRedisPing = function(){
            if(!self.shutdowned){
                self.redisClient.ping(function(err,data){
                    if(!err && data == "PONG"){
                        self.emit("ready")
                    }else{
                        self.emit("error",err)
                        setTimeout(checkRedis,1000)
                    }
                })
            }
        }
        checkRedisPing()

    }else{
        process.nextTick(function(){
            self.emitReadyWhenAllLoaded()
        })
    }
}

BigQueueJournalClient.prototype.connect = function(){
    var self = this;
    try{
        this.redisClient = redis.createClient(this.redisConf.port,this.redisConf.host,this.redisConf.options)
        this.redisClient.retry_delay = 50 || this.redisConf.retryDelay
        this.redisClient.retry_backoff = 1
        this.redisClient.on("error",function(err){
            log.err("Error connecting to ["+JSON.stringify(self.redisConf)+"]")
            err["redisConf"] = self.redisConf
            process.nextTick(function(){
                self.emit("error",err)
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

BigQueueJournalClient.prototype.write = function(node,topic, messageId, message, ttl,callback){
    var self = this
    var timer = log.startTimer()
    this.redisClient.eval(redisScripts["journalWrite"],0,node,topic,messageId,JSON.stringify(message),ttl,function(err,data){
        timer("Journal Write")
        if(err){
            log.err("Error journaling message ["+log.pretty(message)+"], error: "+err)
            if(callback)
                callback(err)
        }else{
            if(callback)
                callback(undefined)
        }
    })
}

BigQueueJournalClient.prototype.retrieveMessages = function(node,topic, idFrom, callback){
    this.redisClient.eval(redisScripts["journalRetrieve"],0,node,topic, idFrom, function(err,data){
        if(err){
            if(callback)
                callback(err)
        }else{
            var res = []
            for(var msg in data){
                res.push(bqutils.redisListToObj(data[msg]))
            }
            if(callback)
                callback(undefined, res)
        }
    })
}

BigQueueJournalClient.prototype.shutdown = function(){
    this.shutdowned = true 
    try{
        this.redisClient.stream.end()
        this.redisClient.quit()
        this.redisClient.closing = true
    }catch(e){}
}

exports.createJournalClient = function(redisConf){
    var cli = new BigQueueJournalClient(redisConf)
    cli.connect()
    return cli;
}

