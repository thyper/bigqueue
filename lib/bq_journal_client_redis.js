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
    var self = this;
    self.emitReadyWhenAllLoaded()

}


BigQueueJournalClient.prototype = new events.EventEmitter();

BigQueueJournalClient.prototype.emitReadyWhenAllLoaded = function(){
    var self = this;
    var scriptsLoaded = (Object.keys(redisScripts).length == TOTAL_SCRIPTS)
    if(scriptsLoaded && this.redisLoaded){
        this.emit("ready")
    }else{
        process.nextTick(function(){
            self.emitReadyWhenAllLoaded()
        })
    }
}

BigQueueJournalClient.prototype.connect = function(){
    var self = this;
    this.redisClient = redis.createClient(this.redisConf.port,this.redisConf.host)
    this.redisClient.retry_delay = 50 || this.redisConf.retryDelay
    this.redisClient.retry_backoff = 1
    this.redisClient.on("error",function(err){
        log.err("Error connecting to ["+log.pretty(this.redisConf)+"]")
        err["redisConf"] = self.redisConf
        process.nextTick(function(){
            self.emit("error",err)
        })
    })
    this.redisClient.on("connect",function(){
        self.emit("connect",self.redisConf)
    })
    this.redisClient.on("ready",function(){
        log.inf("Connected to redis ["+log.pretty(self.redisConf)+"]")
        self.redisLoaded = true;
    })
}

BigQueueJournalClient.prototype.write = function(journalName, messageId, message, ttl,callback){
    var self = this
    var timer = log.startTimer()
    this.redisClient.eval(redisScripts["journalWrite"],0,journalName,messageId,JSON.stringify(message),ttl,function(err,data){
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

BigQueueJournalClient.prototype.retrieveMessages = function(journalName, idFrom, callback){
    this.redisClient.eval(redisScripts["journalRetrieve"],0,journalName, idFrom, function(err,data){
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
    var self = this
    this.redisClient.quit(function(err,data){
        if(!err){
            try{
                self.redisClient.end()
            }catch(e){
                log.err("Error shutting down")
            }
        }
    })
}

exports.createJournalClient = function(redisConf){
    var cli = new BigQueueJournalClient(redisConf)
    cli.connect()
    return cli;
}

