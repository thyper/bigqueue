var events = require("events"),
    ZK = require("zookeeper"),
    ZKMonitor = require("../lib/zk_monitor.js"),
    log = require("node-logging")
var ADDED = "ADDED"
var REMOVED = "REMOVED"
var DATACHANGE = "DATACHANGE"
var FORCEDOWN = "FORCEDOWN"

function BigQueueClusterOrchestrator(zkClient,zkClustersPath,createNodeClientFunction,createJournalClientFunction,checkInterval){
    this.zkClient = zkClient
    this.zkClustersPath = zkClustersPath
    this.createNodeClientFunction = createNodeClientFunction
    this.createJournalClientFunction = createJournalClientFunction
    this.checkInterval = checkInterval
    this.clusters = {}
    this.running = true
    var self = this
    this.zkRules = [
        {regexPath: new RegExp(self.zkClustersPath+"$"), controller: self.clustersController},
        {regexPath: new RegExp(self.zkClustersPath+"/(\\w+)$"), controller: self.createMonitorPath},
        {regexPath: new RegExp(self.zkClustersPath+"/(.*)/topics$"), controller: self.topicsController},
        {regexPath: new RegExp(self.zkClustersPath+"/(.*)/topics/(.*)/consumerGroups$"), controller: self.consumerGroupsController},
        {regexPath: new RegExp(self.zkClustersPath+"/(.*)/nodes$"), controller: self.nodesController},
        {regexPath: new RegExp(self.zkClustersPath+"/(.*)/journals$"), controller: self.journalsController},
                
    ]
    this.nodeMonitor = new ZKMonitor(self.zkClient,self)
        
    this.init()

}

BigQueueClusterOrchestrator.prototype = new events.EventEmitter()

BigQueueClusterOrchestrator.prototype.nodeAdded = function(node){
    log.dbg("Node added in zookeeper ["+log.pretty(node)+"]")
    this.applyRules(ADDED,node)
}
BigQueueClusterOrchestrator.prototype.nodeDataChange = function(node){
    log.dbg("Node Data cahnge in zookeeper ["+log.pretty(node)+"]")
    this.applyRules(DATACHANGE,node)
}
BigQueueClusterOrchestrator.prototype.nodeRemoved = function(node){
    log.dbg("Node Removed from zookeeper ["+log.pretty(node)+"]")
    this.applyRules(REMOVED,node)
}

BigQueueClusterOrchestrator.prototype.applyRules = function(type,node){
    var self = this
    this.zkRules.forEach(function(rule){
        if(rule.regexPath.test(node.path)){
            rule.controller(node,type,rule,self)
        }
    })
}

BigQueueClusterOrchestrator.prototype.createMonitorPath = function(node,type,rule,context){
    if(type == ADDED)
        context.nodeMonitor.pathMonitor(node.path+"/"+node.node)
}

BigQueueClusterOrchestrator.prototype.clustersController = function(node,type,rule,context){
    if(type == ADDED){
        context.clusters[node.node] = {"data":node.data, topics:{}, nodes:{}, journals:{}}
        var clusterPath = node.path+"/"+node.node
        context.nodeMonitor.pathMonitor(clusterPath)
    }
}

BigQueueClusterOrchestrator.prototype.topicsController = function(node,type,rule,context){
    var cluster = node.path.match(/.*\/(.*)\/topics$/)[1]
    if(type = ADDED){
        context.clusters[cluster]["topics"][node.node] = {"data":node.data}
        context.clusters[cluster]["topics"][node.node]["consumerGroups"] = []
        context.nodeMonitor.pathMonitor(node.path+"/"+node.node+"/consumerGroups")
    }

}

BigQueueClusterOrchestrator.prototype.consumerGroupsController = function(node,type,rule,context){
    var pathData = node.path.match(/.*\/(.*)\/topics\/(.*)\/consumerGroups$/)
    var cluster = pathData[1]
    var topic = pathData[2]
    if(type == ADDED){
        if(context.clusters[cluster]["topics"][topic]["consumerGroups"] == undefined){
            context.clusters[cluster]["topics"][topic]["consumerGroups"] = []
        }
        context.clusters[cluster]["topics"][topic]["consumerGroups"].push(node.node)
   }
}

BigQueueClusterOrchestrator.prototype.nodesController = function(node,type,rule,context){
    var cluster = node.path.match(/.*\/(.*)\/nodes$/)[1]
    var data
    try{
        data = JSON.parse(node.data)
    }catch(e){
        log.err("Error parsing data for node ["+log.pretty(node)+"]: "+data)
        return
    }
    if(type == ADDED){
        context.clusters[cluster]["nodes"][node.node] = JSON.parse(node.data)
        context.healthCheckNode(cluster,node.node,data,function(err){
            
        })
    }
    if(type == DATACHANGE){
        context.healthCheckNode(cluster,node.node,data,function(err){
            
        })
    }
}

BigQueueClusterOrchestrator.prototype.journalsController = function(node,type,rule,context){
    var cluster = node.path.match(/.*\/(.*)\/journals$/)[1]
    var data  
    try{
        data = JSON.parse(node.data)
    }catch(e){
        log.err("Error parsing data for journal ["+log.pretty(node)+"]: "+data)
        return
    }
    if(type == ADDED){
        context.clusters[cluster]["journals"][node.node] = JSON.parse(node.data)
        context.healthCheckJournal(cluster,node.node,data,function(err){
            
        })
    }
    if(type == DATACHANGE){
        var newData = JSON.parse(node.data)
        context.healthCheckJournal(cluster,node.node,data,function(err){
            
        })
    }
}

BigQueueClusterOrchestrator.prototype.checkClientConnection = function(createClientFunction,data,callback){
    var self = this
    var client = createClientFunction(data)
    client.on("error",function(){
        callback("Error connecting to ["+JSON.stringify(data)+"]")
        client.shutdown()
    })
    client.on("ready",function(){
        callback(undefined,client)
    })
}

BigQueueClusterOrchestrator.prototype.updateClientStatus = function(clusterId,clientType,clientId,data,err,callback){
    var self = this
    var shouldUpdate = false
    if(err && data.status!="DOWN"){
        data.status="DOWN"
        shouldUpdate = true
    }else if(!err && data.status!="UP"){
        data.status = "UP"
        data.errors = 0
        shouldUpdate = true
    }
    this.clusters[clusterId][clientType][clientId] = data
    if(shouldUpdate){
        var zkPath = this.zkClustersPath+"/"+clusterId+"/"+clientType+"/"+clientId
            self.zkClient.a_set(zkPath,JSON.stringify(data),-1,function(rc, error, stat){
                if(rc!=0){
                    log.err("Error setting status UP to journal ["+clusterId+"] ["+jorunalId+"], error: "+error,true)
                    self.processingError(error)
                    return
                }
                log.inf("State of "+clientType+" ["+clusterId+"] ["+clientId+"] changed to "+data.status)
                callback()
            })

    }else{
        callback()
    }
}

BigQueueClusterOrchestrator.prototype.healthCheckJournal = function(clusterId,journalId,data,callback){
    var self = this
    log.dbg("Checking [cluster:"+clusterId+"] [journal:"+journalId+"] [data:"+log.pretty(data)+"]")
    this.checkClientConnection(this.createJournalClientFunction,data,function(err,client){
        if(client){
            client.shutdown()
        }
        self.updateClientStatus(clusterId,"journals",journalId,data,err,function(){
            callback()
        })
    })
}


BigQueueClusterOrchestrator.prototype.healthCheckNode = function(clusterId,nodeId,data,callback){
    if(data.status == FORCEDOWN){
        callback()
        return
    }
    log.dbg("Checking [cluster:"+clusterId+"] [node:"+nodeId+"] [data:"+log.pretty(data)+"]")
    this.checkClientConnection(this.createNodeClientFunction,data,function(err,client){
        if(client){
            self.checkDataStructure(client,clusterId,nodeId,data,function(err){
                client.shutdown()
                if(err){
                    log.err("Error validating structure of ["+clusterId+"] ["+nodeId+"], error: "+log.pretty(err),true)
                    self.processingError(JSON.stringify(err))
                }
                self.updateClientStatus(clusterId,"node",nodeId,data,err,function(){
                    callback()
                })
            })
        }else{
            self.updateClientStatus(clusterId,"node",nodeId,data,err,function(){
                callback()
            })
        }
    })

 /*   client.on("ready",function(){
        log.dbg("Connected to ["+clusterId+"] ["+nodeId+"]")
        self.checkDataStructure(client,clusterId,nodeId,data,function(err){
            if(err){
                log.err("Error validating structure of ["+clusterId+"] ["+nodeId+"], error: "+log.pretty(err),true)
                self.processingError(JSON.stringify(err))
            }
            if(data.status != "UP"){
                data.status = "UP"
                data.errors = 0
                self.clusters[clusterId]["nodes"][nodeId] = data
                self.zkClient.a_set(zkNodePath,JSON.stringify(data),-1,function(rc, error, stat){
                    if(rc!=0){
                        log.err("Error setting status UP to ["+clusterId+"] ["+nodeId+"], error: "+log.pretty(error),true)
                        self.processingError(error)
                        return
                    }
                    log.inf("State of ["+clusterId+"] ["+nodeId+"] changed to UP")
                    callback()
                })
            }
            log.dbg("Shutting down client")
            client.shutdown()

        })
    })

    client.on("error",function(err){
        log.dbg("Problems connecting to ["+clusterId+"] ["+nodeId+"]")
        if(data.status != "DOWN"){
            data.status = "DOWN"
            self.clusters[clusterId]["nodes"][nodeId] = data
            self.zkClient.a_set(zkNodePath,JSON.stringify(data),-1,function(rc, error, stat){
                if(rc!=0){
                    log.err("Error setting status DOWN to ["+clusterId+"] ["+nodeId+"], error: "+log.pretty(error),true)
                    self.processingError(error)
                    return
                }
               log.inf("State of ["+clusterId+"] ["+nodeId+"] changed to DOWN")
               callback(error)
               client.shutdown()
            })
        }
    })*/
}

BigQueueClusterOrchestrator.prototype.checkDataStructure = function(client,clusterId,nodeId,data,callback){
    if(!this.running){
        callback()
        return
    }
    var clusterTopics = this.clusters[clusterId]["topics"]
    var totals = 0
    var count = 0
    var self = this
    var calls =0
    if(self.checkDataRunning){
        callback()
        return
    }
    this.getNodeData(client,function(err,nodeData){
        self.checkDataRunning = false
        if(err){
            callback(err)
            return
        }
        self.createUnexistentTopics(client,nodeData,clusterId,function(err){
            if(err){
                callback(err)
                return
            }
            self.createUnexistentConsumerGroups(client,nodeData,clusterId,function(err){
                if(err){
                    callback(err)
                    return
                }
                self.checkDataRunning = false
                callback()
            })
        })
    })
}

BigQueueClusterOrchestrator.prototype.createUnexistentTopics = function(client,nodeData,clusterId,callback){
    var topics = Object.keys(this.clusters[clusterId]["topics"])
    var nodeTopics = Object.keys(nodeData)
    var unexistent = topics.filter(function(val){
        return nodeTopics.indexOf(val) < 0
    })
    var opsCount = unexistent.length
    var opsExec = 0
    var finished = false

    if(unexistent.length == 0){
        callback()
        return
    }
    client.listTopics(function(data){
    unexistent.forEach(function(val){
        log.inf("Creating unexistent topic ["+val+"] for cluster ["+clusterId+"] client ["+log.pretty(client)+"]")   
        client.createTopic(val,function(err,data){
            opsExec++
            nodeData[val]=[]
            if(opsExec >= opsCount && !finished){
                finished = true
                callback()
            }
        })
    })
    })
}

BigQueueClusterOrchestrator.prototype.createUnexistentConsumerGroups = function(client,nodeData,clusterId,callback){
    var consumerGroupsToCreate = [] 
    var allTopics = this.clusters[clusterId]["topics"]
    var topicKeys = Object.keys(allTopics)
    for(var i in topicKeys){
        var topicKey = topicKeys[i]
        var consumers = allTopics[topicKey]["consumerGroups"]
        for(var j in consumers){
            var consumer = consumers[j]
            if(!nodeData[topicKey] || nodeData[topicKey].indexOf(consumer) < 0){
                consumerGroupsToCreate.push({"topic":topicKey,"consumer":consumer})
            }
        }
    }
    
    var opsCount = consumerGroupsToCreate.length
    var opsExec = 0
    var finished = false
    if(opsCount == 0){
        callback()
        return
    }
    
    consumerGroupsToCreate.forEach(function(val){
        log.inf("Creating unexistent group ["+log.pretty(val)+"] for cluster ["+clusterId+"] client ["+log.pretty(client)+"]")   
        client.createConsumerGroup(val.topic,val.consumer,function(err,data){
            opsExec++
            if(err)
                log.err("Error creating group ["+log.pretty(err)+"]")
            if(opsExec >= opsCount && !finished){
                callback()
                finished = true
            }
        })
    })

}

BigQueueClusterOrchestrator.prototype.getNodeData = function(client,callback){
    var nodeData = {}
    var opsCount = 0
    var opsExec = 0
    var finished = false
    client.listTopics(function(data){
        for(var i = 0; i<data.length;i++){
            opsCount++
            nodeData[data[i]] = []
        }
        if(opsCount == 0){
            callback(undefined,nodeData)
        }else{
            data.forEach(function(val){
                nodeData[val] = []
                client.getConsumerGroups(val,function(err,data){
                    if(finished)
                        return
                    opsExec++
                    if(err && !finished){
                        finished = true
                        callback(err,undefined)
                        return
                    }else{
                        nodeData[val] = data
                    }
                    if(opsExec >= opsCount && !finished){
                        finished = true
                        callback(undefined,nodeData)
                    }
                })
            })
        }
    })
}

BigQueueClusterOrchestrator.prototype.periodicalCheck = function(context){
    var clusterKeys = Object.keys(context.clusters)
    for(var i in clusterKeys){
        var clusterKey = clusterKeys[i]
        var cluster = context.clusters[clusterKey]
        var nodesKeys = Object.keys(cluster["nodes"])
        for(var j in nodesKeys){
            var nodeKey = nodesKeys[j]
            context.healthCheckNode(clusterKey,nodeKey,context.clusters[clusterKey]["nodes"][nodeKey],function(err){
            })
        }
    }
}

BigQueueClusterOrchestrator.prototype.processingError = function(error){
    console.trace("Error processing ["+error+"]")
    process.exit(1)
}
BigQueueClusterOrchestrator.prototype.shutdown = function(){
    this.running = false
    clearInterval(this.intervalId)
    this.nodeMonitor.running = false
}
BigQueueClusterOrchestrator.prototype.init = function(){
    var self = this
    this.zkClient.connect(function(err){
        if(err){
            log.err("Error connecting to zookeeper ["+log.pretty(err)+"]",true)
            self.emit("error",err)
            return
        }
        self.nodeMonitor.pathMonitor(self.zkClustersPath)
        setTimeout(function(){
            self.intervalId = setInterval(function(){
                self.periodicalCheck(self)
            },self.checkInterval)
        },self.checkInterval)
        self.emit("ready")
    })

}

exports.createOrchestrator = function(conf){
   var zk = new ZK(conf.zkConfig)
   log.setLevel(conf.logLevel || "info")
   var orch = new BigQueueClusterOrchestrator(zk,conf.zkClustersPath,conf.createNodeClientFunction,conf.createJournalClientFunction,conf.checkInterval)
   return orch
}
