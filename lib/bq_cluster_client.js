var fs = require('fs'),
    should = require('should'),
    events = require("events"),
    ZK = require ("zookeeper"),
    ZKMonitor = require("../lib/zk_monitor.js"),
    metricCollector = require('../lib/metricsCollector.js'),
    os = require("os"),
    log = require("node-logging")

var topicsPath = "/topics"
var consumerGroupsPath = "/consumerGroups"

//Default ttl 3 days
var defaultTtl = 3*24*60*60

var DOWN_STATUS="DOWN";

var buckets = ["< 5",">= 5 && < 10",">= 10 && < 20",">= 20 && < 30",">= 30 && < 40",">= 40 && < 50",">= 50 && < 60",">= 60 && < 70", ">= 70 && < 80",">= 80 && < 90",">= 90 && < 100",">= 100"]
var shouldCount = function(val){
           if(val < 5)
               return "< 5"
           else if(val >= 5 && val < 10)
               return ">= 5 && < 10"
           else if(val >= 10 && val < 20)
               return ">= 10 && < 20"
           else if(val >= 20 && val < 30)
               return ">= 20 && < 30"
           else if(val >= 30 && val < 40)
               return ">= 30 && < 40"
           else if(val >= 40 && val < 50)
               return ">= 40 && < 50"
           else if(val >= 50 && val < 60)
               return ">= 50 && < 60"
           else if(val >= 60 && val < 70)
               return ">= 60 && < 70"
           else if(val >= 70 && val < 80)
               return ">= 70 && < 80"
           else if(val >= 80 && val < 90)
               return ">= 80 && < 90"
           else if(val >= 90 && val < 100)
               return ">= 90 && < 100"
           else
               return ">= 100"
       }

var statsInit = exports.statsInit = false
var clusterStats
var getStats
var postStats
var ackStats
var failStats
var clientStats
var journalStats

/**
 *  Object responsible for execute all commands throght the bq cluster
 */
function BigQueueClusterClient(zkClient,zkClusterPath,zkFlags,createClientFunction,createJournalFunction,sentCacheTime,refreshTime,clientTimeout,statsInterval,statsFile){
    if(!exports.statsInit){
        exports.statsInit = true
        clusterStats = metricCollector.createCollector("clusterStats",buckets,shouldCount,statsFile,statsInterval)
        getStats = metricCollector.createCollector("getStats",buckets,shouldCount,statsFile,statsInterval)
        postStats = metricCollector.createCollector("postStats",buckets,shouldCount,statsFile,statsInterval)
        ackStats = metricCollector.createCollector("ackStats",buckets,shouldCount,statsFile,statsInterval)
        failStats = metricCollector.createCollector("failStats",buckets,shouldCount,statsFile,statsInterval)
        clientStats = metricCollector.createCollector("clientStats",buckets,shouldCount,statsFile,statsInterval)
        journalStats = metricCollector.createCollector("journalStats",buckets,shouldCount,statsFile,statsInterval)
    }
    this.nodes=[]
    this.journals=[]
    this.statsInterval = statsInterval || -1
    this.statsFile = statsFile
    this.zkClient = zkClient
    this.zkClusterPath = zkClusterPath
    this.zkFlags = 0 | zkFlags
    this.createClientFunction = createClientFunction
    this.createJournalFunction = createJournalFunction
    this.loading = true
    this.nodesPath = this.zkClusterPath+"/nodes"
    this.journalsPath = this.zkClusterPath+"/journals"
    this.sentCacheTime = sentCacheTime || 60
    this.clientTimeout = clientTimeout || 125
    this.hostname = os.hostname()
    this.uid = 0
    this.sentCache = {}
    this.controlZookeeper = false
    //If the property is not set we'll use 60s of refresh time
    this.refreshTime = refreshTime || 10000
    this.shutdowned = false
    var self = this

    //Metada about the distribution of the clients by zookeeper path
    this.clientsMeta = {}
    this.clientsMeta[this.nodesPath]= {
        "createClient": self.createClientFunction,
        "clientsList": "nodes"
    }
    this.clientsMeta[this.journalsPath]= {
        "createClient": self.createJournalFunction,
        "clientsList": "journals"
    }
    this.prepare = function(shouldCallReady){
        if(self.shutdowned)
            return
        if(shouldCallReady == undefined)
            shouldCallReady = true
        if(self.nodeMonitor)
            self.nodeMonitor.shutdown()
        self.nodeMonitor = new ZKMonitor(self.zkClient,self)
        self.nodeMonitor.on("error",function(err){
            self.emit("error",err)
        })
        self.nodeMonitor.pathMonitor(self.nodesPath)
        self.nodeMonitor.pathMonitor(self.journalsPath)
        //Check if no nodes
        self.zkClient.a_get_children(self.nodesPath,false,function(rc,error,childrens){
            if(shouldCallReady && rc == 0 && childrens.length == 0){
                self.callReadyIfFinish()
            }
        })

    }
    //Check if zookeeper is already connected
    if(this.zkClient.state != 3){
        log.inf("Connecting to zookeeper")
        this.zkClient.connect(function(err){
            if(err){
                log.err("Error connecting to zookeeper ["+log.pretty(err)+"]")
                self.emit("error",err)
                return
            }
            self.controlZookeeper = true
            self.prepare()
        })
    }else{
        this.prepare()
    }

}

BigQueueClusterClient.prototype = new events.EventEmitter()


BigQueueClusterClient.prototype.getClientById = function(list, id){
    for(var i=0; i<list.length; i++){
        if(list[i] && list[i].id == id)
            return list[i]     
    }
}
/**
 * Look for a node with specific id
 */
BigQueueClusterClient.prototype.getNodeById = function(id){
    return this.getClientById(this.nodes,id)
}
BigQueueClusterClient.prototype.getJournalById = function(id){
    return this.getClientById(this.journals,id)
}

BigQueueClusterClient.prototype.shutdown = function(){
    this.shutdowned = true
    this.nodeMonitor.running = false
    clearInterval(this.refreshInterval)
    if(this.zkClient && this.controlZookeeper){
        this.zkClient.close()
    }
    for(var i in this.clientsMeta){
        var type = this.clientsMeta[i]
        var clients = this[type.clientsList]
        for(var j in clients){
            clients[j].client.shutdown()
        }
    }
}
/**
 * When node is added we0ll create a clusterNode object and if is loading we will notify this load
 */
BigQueueClusterClient.prototype.nodeAdded = function(node){

    var meta = this.clientsMeta[node.path]
    var clusterNode = this.getClientById(this[meta.clientsList],node.node)
    if(!clusterNode){
        var self = this
        clusterNode = {}
        clusterNode["id"] = node.node
        clusterNode["data"] = JSON.parse(node.data)
        clusterNode["sentMessages"] = {}
        
        //If added node is down we'll break the execution
        if(clusterNode.data.status == DOWN_STATUS) {
          return;
        }
        
        clusterNode["client"] = meta.createClient(clusterNode.data)
        if(!clusterNode.host || !clusterNode.port){
           var portPos = node.node.lastIndexOf("-")
           clusterNode.host = node.node.substring(0,portPos)
           clusterNode.port = node.node.substring(portPos+1)
        }
        
        this[meta.clientsList].push(clusterNode)
        clusterNode.client.on("ready",function(err){
            //If it's loading will emit a signal to callReadyIfFinish 
            //and we'll check internally the readyEmited to the signal only once
            if(self.loading && !this.readyEmited){
                this.readyEmited = true
                self.callReadyIfFinish()
            }

        })
        clusterNode.client.on("error",function(err){
            if(self.loading && !this.readyEmited){
                this.readyEmited = true
                self.callReadyIfFinish()
            }

            log.err(err)
        })
        log.inf("Zookeeper node added ["+log.pretty(node)+"]")
    }else{
        this.nodeDataChange(node)
    }

}

/**
 * Filter the nodes list by UP status
 */
BigQueueClusterClient.prototype.getNodesNoUp = function(){
    return this.nodes.filter(function(val){
        return val && val.data && val.data.status != "UP"
    })
}

/**
 * Check if all is loaded and emit the "ready" event
 */
BigQueueClusterClient.prototype.callReadyIfFinish = function(){
    var self = this
    if(this.loaded == undefined)
        this.loaded = 0
    this.loaded++;
    if(this.loaded >= (this.nodeMonitor.monitoredPaths(this.nodesPath).length + 
        this.nodeMonitor.monitoredPaths(this.journalsPath).length)) {
       this.loading = false

       //Refresh status
       if(this.refreshTime > 0){
           this.refreshInterval = setInterval(function(){
               if(!self.shutdowned){
                   process.nextTick(function(){
                       self.nodeMonitor.refresh()
                   })
               }else{
                   clearInterval(interval)
               }
           },this.refreshTime)
       }

       this.emit("ready")
    }
}


/**
 * When a node is removed we'll remove it from the nodes list
 * and re-send all messages sent by this node
 */
BigQueueClusterClient.prototype.nodeRemoved = function(node){
    var meta = this.clientsMeta[node.path]
    var clusterNode = this.getClientById(this[meta.clientsList],node.node)
    if(!clusterNode)
        return
    if(clusterNode["shutdown"] != undefined)
        clusterNode.shutdown()
    this[meta.clientsList] = this[meta.clientsList].filter(function(val){
        return val.id != clusterNode.id
    })
    log.inf("Zookeeper node removed ["+log.pretty(node)+"]")
}

/**
 * When a node data is changes we'll load it and if the node chage their status 
 * from up to any other we'll re-send their cached messages
 */
BigQueueClusterClient.prototype.nodeDataChange = function(node){
    var meta = this.clientsMeta[node.path]
    var newData = JSON.parse(node.data)
    var clusterNode = this.getClientById(this[meta.clientsList],node.node)
    if(!clusterNode){
        this.nodeAdded(node)
    }else{
        var oldData = clusterNode.data
        if(clusterNode.data != newData) {
          clusterNode.data = newData
          log.inf("Zookeeper node data chaged ["+log.pretty(node)+"]")
          if(newData.status == DOWN_STATUS) {
            this.nodeRemoved(node);
          }
        }
    }
}

/**
 * Call execute a function with all nodes and call callback when all execs are finished
 * the way to detect finishes are using a monitor that will be called by the exec function
 */
BigQueueClusterClient.prototype.withEvery = function(list,run,callback){
    var count = 0
    var total = list.length
    var hasErrors = false
    var errors = []
    var datas = []
    if(list.length == 0)
        callback()
    var monitor = function(err,data){
        if(err){
            hasErrors = true;
            errors.push({"msg":err})
        }
        if(data){
            datas.push(data)
        }
        count++
        if(count >= total){
            if(hasErrors)
                callback(errors,datas)
            else
                callback(undefined,datas)
        }
    }
    for(var i in list){
        run(list[i],monitor)
    }
}

/**
 * Get one node in a round-robin fashion
 */
BigQueueClusterClient.prototype.nextNodeClient = function(){
    var node = this.nodes.shift()
    this.nodes.push(node)
    return node
}

BigQueueClusterClient.prototype.generateClientUID = function(){
    return this.hostname+":"+(new Date().getTime())+":"+(this.uid++)
}

/**
 * Exec a function with one client if an error found we try with other client until
 * there are nodes into the list if the function fails with all nodes the callback will be
 * called with an error
 */
BigQueueClusterClient.prototype.withSomeClient = function(run,cb){
    var self = this
    var calls = 0
    var actualClient
    function monitor(){
        this.timer = log.startTimer()
        var m = this
        m.emited = false
        setTimeout(function(){
            if(!m.emited){
                m.timer("timeout found")
                log.inf("Timeout on client: "+log.pretty(actualClient))
                m.emit("timeout",undefined)
            }
        },self.clientTimeout)

        this.emit = function(err,data,finishWithNoData){
            var shouldEmitWithNoData = true
            if(finishWithNoData != undefined)
                shouldEmitWithNoData = finishWithNoData

            m.timer("Monitor emit called")
            if(m.emited)
                return
            m.emited = true
            calls++
            if(!err && shouldEmitWithNoData){
                m.timer("Finishing monitor")
                cb(err,data)
                return
            }
            if(calls < self.nodes.length){
                actualClient = self.nextNodeClient()
                run(actualClient,new monitor())
            }else{
                var error = err
                m.timer("Finishing monitor exec with errors ["+error+"] ")
                if(error) {
                  var msg = error instanceof Object ? JSON.stringify(error) : error;
                  cb({msg:"Node execution fail ["+msg+"]"},undefined)
                } else {
                    cb()
                }
            }
        }
        this.next = function(){
            this.emit(undefined,undefined,false)
        }
    }

    if(this.nodes.length == 0){
        cb({err:"No nodes found"},null)
        return
    }
    actualClient = this.nextNodeClient()
    run(actualClient,new monitor())
}

/**
 * Creates a topic into all nodes
 */
BigQueueClusterClient.prototype.createTopic = function(){
    var topic = arguments[0]
    var callback
    var ttl = undefined
    if(arguments.length == 2){
        callback = arguments[1]
    }else{
        ttl = arguments[1]
        callback = arguments[2]
    }
    if(this.getNodesNoUp().length > 0){
        callback({msg:"There are nodes down, try in few minutes"})
        return
    }
    if(ttl == undefined){
        ttl = defaultTtl 
    }
    var self = this
    var topicPath = this.zkClusterPath+topicsPath+"/"+topic
    var topicConsumerPath = this.zkClusterPath+topicsPath+"/"+topic+"/consumerGroups"
    //Check if exist in zookeeper
    this.zkClient.a_exists(topicPath,false,function(rc,error,stats){
            if(rc == ZK.ZNONODE){
                //If not exist create topic on each node
                self.withEvery(self.nodes,function(clusterNode,monitor){
                    clusterNode.client.createTopic(topic,ttl,monitor);
                },
                function(err){
                    /*
                     *If there are any error running the create throw an error, 
                     *the orchestor should remove the unexistent topics from the redis that could create it
                     */
                    if(err){
                        log.err("Error creating topic ["+log.pretty(err)+"]")
                        err = {"msg":err}
                        callback(err)
                    }else{
                        self.zkClient.a_create(topicPath,JSON.stringify({"ttl":ttl}),self.zkFlags,function(rc, error, path){
                            if(rc!=0){
                                log.err("Creating path in zookeeper ["+topicPath+"],error: "+error)
                                callback({"msg":"Error registering topic ["+topic+"] into zookeeper"})
                            }else{
                                self.zkClient.a_create(topicConsumerPath,"{}",self.zkFlags,function(rc, error, path){
                                    if(rc!=0){
                                        log.err("Creating path in zookeeper ["+topicConsumerPath+"], error: "+log.pretty(error))
                                        callback({"msg":"Error registering topic ["+topic+"] into zookeeper"})
                                    }else{
                                        callback()
                                    }
                                })
                            }
                        })
                    }
                })
            }else{
                //If already exist throw error
                var msg = topic
                if(msg instanceof Object)
                    msg = JSON.stringify(msg)
                callback({"msg":"Error topic ["+msg+"] already exist"})
            }
    })
}

BigQueueClusterClient.prototype.deleteTopic = function(topic,callback){
    var self = this
    var topicPath = this.zkClusterPath+topicsPath+"/"+topic
    var topicConsumerPath = this.zkClusterPath+topicsPath+"/"+topic+"/consumerGroups"
    //Check if exist in zookeeper
    if(this.getNodesNoUp().length > 0){
        callback({msg:"There are nodes down, try in few minutes"})
        return
    }
    this.zkClient.a_exists(topicPath,false,function(rc,error,stats){
        if(rc!=0){
            return callback({"msg":"Topic doesn't exist ["+topic+"]","code":404})
        }
        self.zkClient.a_get_children(topicConsumerPath,false,function(rc,error,children){
            if(rc!=0)
                return callback({msg:"Error reading from zookeeper ["+topicsConsumerPath+"], "+rc+"-"+error})
            if(children.length > 0)
                return callback({"msg":"Topic ["+topic+"] contains consumers","code":409})

            //Delete consumer folder    
            self.zkClient.a_delete_(topicConsumerPath,-1,function(rc, error){
                if(rc!=0){
                    log.err("Deletin path in zookeeper ["+topicConsumerPath+"],error: "+error)
                    callback({msg:"Error deleting topic ["+topic+"] from zookeeper"})
                }else{

                    //Delete topic folder
                    self.zkClient.a_delete_(topicPath,-1,function(rc, error){
                        if(rc!=0){
                            log.err("Deleting path in zookeeper ["+topicPath+"], error: "+log.pretty(error))
                            callback({msg:"Error deleting topic ["+topic+"] from zookeeper"})
                        }else{
                            
                            //When data is deleted from zookeeper we'll delete it from nodes
    
                            self.withEvery(self.nodes,function(clusterNode,monitor){
                                clusterNode.client.deleteTopic(topic,function(err){
                                    monitor(err)
                                })
                            },
                            function(err){
                                if(err){
                                    log.err("Error deleting topic ["+log.pretty(err)+"]")
                                    err = {"msg":err}
                                    callback(err)
                                }else{
                                    callback()
                                }
                            })
                        }
                    })
                }
            })
        })
    })
}

/**
 * Creates a consumer group into all nodes
 */
BigQueueClusterClient.prototype.createConsumerGroup = function(topic,consumer,callback){
    if(this.getNodesNoUp().length > 0){
        callback({msg:"There are nodes down, try in few minutes"})
        return
    }
    var self = this
    var consumerPath  = this.zkClusterPath+topicsPath+"/"+topic+"/consumerGroups/"+consumer
    //Check if exist in zookeeper
    this.zkClient.a_exists(consumerPath,false,function(rc,error,stats){
            if(rc == ZK.ZNONODE){
                //If not exist create consumer on all nodes
                self.withEvery(self.nodes,function(clusterNode,monitor){
                    clusterNode.client.createConsumerGroup(topic,consumer,function(err){
                        if(err){
                            monitor(err)
                        }else{
                            monitor()
                        }
                    })
                },
                function(err){
                    /*
                     *If there are any error running the create throw an error, 
                     *the orchestor should remove the unexistent topics from the redis that could create it
                     */
                    if(err){
                        log.err("Error creating consumer group [topic:"+topic+"] [consumer:"+consumer+"], error: "+log.pretty(err))
                        err = {"msg":err}
                        callback(err)
                        return
                    }
                    self.zkClient.a_create(consumerPath,"",self.zkFlags,function(rc, error, path){
                        if(rc!=0){
                            log.error("Creating path in zookeeper ["+consumerPath+"], error: "+log.pretty(error))
                            callback({msg: "Error registering consumer group ["+consumer+"] for topic ["+topic+"] into zookeeper"})
                            return
                        }
                        callback()
                    })
                })
            }else{
                //If already exist throw error
                callback({msg:"Error consumer group ["+consumer+"] for topic ["+topic+"] already exist","code":409})
            }
    })

}

/**
 * Reset consumer group in all redis
 */
BigQueueClusterClient.prototype.resetConsumerGroup = function(topic,consumer,callback){
     if(this.getNodesNoUp().length > 0){
        callback({msg:"There are nodes down, try in few minutes"})
        return
    }
    var self = this
    var consumerPath  = this.zkClusterPath+topicsPath+"/"+topic+"/consumerGroups/"+consumer
    //Check if exist in zookeeper
    this.zkClient.a_exists(consumerPath,false,function(rc,error,stats){
        if(rc == ZK.ZNONODE){
            return callback({"msg":"Consumer ["+consumer+"] for topic ["+topic+"] does not exists","code":404})
        }
        self.withEvery(self.nodes,function(clusterNode,monitor){
            clusterNode.client.resetConsumerGroup(topic,consumer,function(err){
                if(err){
                    monitor(err)
                }else{
                    monitor()
                }
            })
        },
        function(err){
            callback(err)  
        })

    })
     
}


BigQueueClusterClient.prototype.deleteConsumerGroup = function(topic,consumer,callback){
     if(this.getNodesNoUp().length > 0){
        callback({msg:"There are nodes down, try in few minutes"})
        return
    }
    var self = this
    var consumerPath  = this.zkClusterPath+topicsPath+"/"+topic+"/consumerGroups/"+consumer
    //Check if exist in zookeeper
    this.zkClient.a_exists(consumerPath,false,function(rc,error,stats){
        if(rc == ZK.ZNONODE){
            return callback({"msg":"Consumer ["+consumer+"] for topic ["+topic+"] does not exists","code":404})
        }
        self.zkClient.a_delete_(consumerPath,-1,function(rc, error){
            if(rc != 0){
                var err = "Error deleting path from zookeeper ["+error+"]"
                log.err(err)
                err = {"msg":err}
                return callback(err)
            }
            self.withEvery(self.nodes,function(clusterNode,monitor){
                clusterNode.client.deleteConsumerGroup(topic,consumer,function(err){
                    if(err){
                        monitor(err)
                    }else{
                        monitor()
                    }
                })
            },
            function(err){
                callback(err)  
            })

        })
    })
     
}

/**
 * Get unix timestamp
 */
BigQueueClusterClient.prototype.tms = function(){
    var d = new Date().getTime()/1000
    return Math.floor(d)
}

/**
 * Post a message to one node generating a uid 
 */
BigQueueClusterClient.prototype.postMessage = function(topic,message,callback){
    var startTime = new Date().getTime()
    var timer = log.startTimer()
    var self = this
    var uid = this.generateClientUID()
    message["uid"] = uid
    this.withSomeClient(
        function(clusterNode,monitor){
            if(!clusterNode || clusterNode.data.status != "UP"){
                monitor.emit("Error with clusterNode",undefined)
                return
            }
            if(clusterNode.data.read_only){
                log.dbg("Node ["+JSON.stringify(clusterNode.data)+"] is in readonly")
                return monitor.next()
            }
            var clientTimer = clientStats.timer() 
            clusterNode.client.postMessage(topic,message,function(err,key){
                clientStats.collect(clientTimer.lap())
                if(!err){
                    //If no error, write the data to the journals and load it to the local cache
                    key["uid"] = uid
                    //Load to journal
                    var journalTimer = journalStats.timer()
                    self.writeMessageToJournal(topic,key.id,message,clusterNode,function(err){
                        journalStats.collect(journalTimer.lap())
                        monitor.emit(err,key)
                    })
                    
                }else{
                    monitor.emit(err,key)
                    log.err("Error posting message ["+log.pretty(err)+"] ["+log.pretty(clusterNode.data)+"] [topic:"+topic+"]")
                }
            })
        },
        function(err,key){
            var time = new Date().getTime() - startTime
            clusterStats.collect(time)
            postStats.collect(time)

            timer("Post message to cluster")
            callback(err,key)
            timer("Posted cluster callback")
        }
   )
}

BigQueueClusterClient.prototype.writeMessageToJournal = function(topic, msgId, message, clusterNode, cb){
    var self = this
    if(!clusterNode.data.journals || clusterNode.data.journals.length == 0 ){
        cb(undefined)
    }else{
        var journalIds = clusterNode.data.journals
        var journals = []
        for(var i in journalIds){
            var journal = this.getJournalById(journalIds[i])
            if(!journal){
                var err = "Error getting journal ["+journalIds[i]+"] warranties not gotten" 
                log.err(err)
                err = {"msg":err}
                return cb(err,undefined)
            }
            if(journal.data.status != "UP"){
                var err = "Error journal ["+journalIds[i]+"] DOWN" 
                log.err(err)
                err = {"msg":err}
                return cb(err,undefined)
            }
            journals.push(journal)
        }
        this.withEvery(journals,
            function(journal, jMonitor){
                journal.client.write(clusterNode.id,topic,msgId,message,self.journalTtl,function(err){
                    jMonitor(err)
                })
            },
            function(err,data){
                cb(err)
            }
        )
    }
}

/**
 * Return one message queued for a consumer group, this method will generates a recipientCallback 
 * that we'll be used 
 */
BigQueueClusterClient.prototype.getMessage = function(topic,group,visibilityWindow,callback){
    var self = this
    var metricsTimer = clusterStats.timer()
    var timer = log.startTimer()
    timer("Getting message")
    this.withSomeClient(
        function(clusterNode,monitor){     
            if(!clusterNode || (clusterNode.data.status != "UP")){
                monitor.emit("Cluster node error or down ["+clusterNode.data.host+"]")
                return
            }
            self.getMessageFromNode(clusterNode.id, topic, group, visibilityWindow, function (err, data) {
              monitor.emit(err, data);
            });
        },
        function(err,data){
            timer("Get message from cluster")
            if(data == {})
                data = undefined
            if(err){
                log.err("Error getting messages ["+err.msg+"]")
                err = {"msg":err.msg}
            }
            clusterStats.collect(metricsTimer.lap())
            getStats.collect(metricsTimer.lap())
            if(data == undefined)    
                callback(err,data)
            else
                callback(undefined,data)
            timer("Getted Message from cluster Callback end")
        }
    )
}
BigQueueClusterClient.prototype.createRecipientCallback = function(nodeId, topic, group, msgId) {
  return this.encodeRecipientCallback({"nodeId":nodeId,"topic":topic,"consumerGroup":group,"id":msgId})

}
BigQueueClusterClient.prototype.getMessageFromNode = function(nodeId,topic,group,visibilityWindow,callback){
  var self = this;
  var clusterNode = this.getNodeById(nodeId);
  if(!clusterNode) {
    return callback({"msg":"Node ["+nodeId+"] not found"});
  }
  if(clusterNode.data.status != "UP") {
    return callback({"msg":"Node ["+nodeId+"] is not UP ["+clusterNode.data.status+"]"});
  }
  var clientTimer = clientStats.timer()
  clusterNode.client.getMessage(topic,group,visibilityWindow,function(err,data){
      clientStats.collect(clientTimer.lap())
      if(err){
          callback(err,undefined)
      }else{
          if(data.id){
              data["recipientCallback"] = self.createRecipientCallback(nodeId, topic, group, data.id);
              data["nodeId"] = clusterNode.id;
              callback(undefined,data);
          }else{
              callback(undefined,undefined);
          }
      }
  })

}
/**
 * Generates the recipient callback to return at get instance
 */
BigQueueClusterClient.prototype.encodeRecipientCallback = function(data){
    var keys = Object.keys(data)
    var strData = ""
    for(var i in keys){
        strData = strData+":"+keys[i]+":"+data[keys[i]]
    }
    return strData.substring(1)
}

/**
 * Regenerate the data ecoded by #encodeRecipientCallback
 */
BigQueueClusterClient.prototype.decodeRecipientCallback = function(recipientCallback){
   var splitted = recipientCallback.split(":")
   var data = {}
   for(var i = 0; i< splitted.length; i=i+2){
        data[splitted[i]] = splitted[i+1]
   }
   return data
}

BigQueueClusterClient.prototype.ackMessage = function(topic,group,recipientCallback,callback){
    var startTime = clusterStats.timer()
    var repData = this.decodeRecipientCallback(recipientCallback)
    var node = this.getNodeById(repData.nodeId)
    if(!node || node.data.status != "UP"){
        callback({msg:"Node ["+repData.nodeId+"] not found or down","code":404})
        return
    }
    node.client.ackMessage(topic,group,repData.id,function(err){
        var time = new Date().getTime() - startTime
        clientStats.collect(startTime.lap())
        clusterStats.collect(startTime.lap())
        ackStats.collect(startTime.lap())

        if(err){
            log.err("Error doing ack of recipientCallback ["+recipientCallback+"], error: "+err.msg)
            err = {"msg":err.msg}
            return callback(err)
        }
        callback()
     })
}

BigQueueClusterClient.prototype.failMessage = function(topic,group,recipientCallback,callback){
    var startTime = clientStats.timer() 
    var repData = this.decodeRecipientCallback(recipientCallback)
    var node = this.getNodeById(repData.nodeId)
    if(!node || node.data.status != "UP"){
        callback({msg:"Node ["+repData.nodeId+"] not found or down","code":404})
        return
    }
    node.client.failMessage(topic,group,repData.id,function(err){
        clientStats.collect(startTime.lap())
        clusterStats.collect(startTime.lap())
        failStats.collect(startTime.lap())

        if(err){
            err = {"msg":err}
            log.err("Error doing fail of recipientCallback ["+recipientCallback+"], error: "+log.pretty(err))
        }
        callback(err)
    })
}

BigQueueClusterClient.prototype.listTopics = function(callback){
    var topicsPath = this.zkClusterPath+"/topics"
    this.zkClient.a_get_children(topicsPath,false,function(rc,error,children){
        if(rc!=0){
            log.err("Error listing topics ["+log.pretty(error)+"]")
        }
        callback(children)
    })
}

BigQueueClusterClient.prototype.getConsumerGroups = function(topic,callback){
    if(this.shutdowned)
        return callback("Client shutdowned")
    var consumerGroupsPath = this.zkClusterPath+"/topics/"+topic+"/consumerGroups"
    //Check first if node exists
    var self = this
    this.zkClient.a_exists(consumerGroupsPath,false,function(rc,error){
        if(rc != 0){
            callback({"msg":"Path ["+consumerGroupsPath+"], doesn't exists","code":404})
            return
        }
        self.zkClient.a_get_children(consumerGroupsPath,false,function(rc,error,children){
            if(rc!=0){
                log.err("Error getting consumer groups ["+log.pretty(error)+"]")
                callback({msg:"Error ["+rc+"-"+error+", "+consumerGroupsPath+"]"},undefined)
                return
            }
            callback(undefined,children)
        })
    })
}

BigQueueClusterClient.prototype.getConsumerStats = function(topic,consumer,callback){
    this.withEvery(this.nodes,
        function(node,monitor){
            node.client.getConsumerStats(topic,consumer,function(err,data){
                monitor(err,data)
           })
        },
        function(err,data){
            if(err && (!data || data.length <=0)){
                callback({"msg":JSON.stingify(err)},undefined)
                return
            }
            var sumarized = {}
            if(data){
                data.forEach(function(elem){
                    var keys = Object.keys(elem)
                    keys.forEach(function(val){
                        if(!sumarized[val]){
                            sumarized[val]=0
                        }
                        sumarized[val]+=elem[val]
                    })
                })
            }
            callback(undefined,sumarized)
        }
    )
}

BigQueueClusterClient.prototype.getTopicTtl = function(topic,callback){
    this.withSomeClient(
        function(clusterNode,monitor){
                if(!clusterNode || (clusterNode.data.status != "UP")){
                    monitor.emit("Error with clusterNode",null)
                    return
                }
                clusterNode.client.getTopicTtl(topic,function(err,data){
                if(err){
                    monitor.emit(err,undefined)
                }else{
                    monitor.emit(err,data)
                }
            })
        },
        function(err,data){
            if(err)
                err = {"msg":err}
            callback(err,data)
        }
   )

}

exports.createClusterClient = function(clusterConfig){
    var zk
    //if we send a zk object we'll share the connection with the object
    if(clusterConfig.zk){
        log.inf("Using zookeeper shared connection")
        zk = clusterConfig.zk
    }else{
        log.inf("Using new zookeeper connection")
        zk = new ZK(clusterConfig.zkConfig)
    }
    return  new BigQueueClusterClient(zk,clusterConfig.zkClusterPath,
                                            clusterConfig.zkFlags,
                                            clusterConfig.createNodeClientFunction,
                                            clusterConfig.createJournalClientFunction,
                                            clusterConfig.sentCacheTime,
                                            clusterConfig.refreshTime,
                                            clusterConfig.clientTimeout,
                                            clusterConfig.statsInterval,
                                            clusterConfig.statsFile)
} 
