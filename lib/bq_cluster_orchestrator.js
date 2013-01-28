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
    if(type == ADDED){
        context.clusters[cluster]["topics"][node.node] = {"data":node.data}
        context.clusters[cluster]["topics"][node.node]["consumerGroups"] = []
        context.nodeMonitor.pathMonitor(node.path+"/"+node.node+"/consumerGroups")
    }else if(type == REMOVED){
        delete context.clusters[cluster]["topics"][node.node]
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
   }else if(type == REMOVED){
       delete context.clusters[cluster]["topics"][topic]["consumerGroups"][node.node]
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

/**
 *  Try to create a client connection and call the callback with the create client if 
 *  it was possible, if not will call callback with the error
 */
BigQueueClusterOrchestrator.prototype.checkClientConnection = function(createClientFunction,data,callback){
    var self = this
    var client = createClientFunction(data)
    client.on("error",function(){
        callback("Error connecting to ["+JSON.stringify(data)+"]")
        try{
            client.shutdown()
        }catch(e){
            log.error("Error shutting down, "+e)
        }
    })
    client.on("ready",function(){
        callback(undefined,client)
    })
}



/**
 * Validate if the status has changed and update it to zookeeper
 */
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
    if(shouldUpdate){
            var zkPath = this.zkClustersPath+"/"+clusterId+"/"+clientType+"/"+clientId
            self.zkClient.a_set(zkPath,JSON.stringify(data),-1,function(rc, error, stat){
                if(rc!=0){
                    console.log(zkPath)
                    log.err("Error setting status UP to journal ["+clusterId+"] ["+clientId+"], error: "+error)
                    
                    //Check if zookeeper is not closing
                    if(rc != -116){
                        self.processingError(error)
                        return
                    }else{
                        callback("Error, ["+data+"] has not been updated");
                    }
                }
                log.inf("State of "+clientType+" ["+clusterId+"] ["+clientId+"] changed to "+data.status)
                callback()
            })

    }else{
        callback()
    }
    this.clusters[clusterId][clientType][clientId] = data

}

/**
 * Check the status of an specific journal
 */
BigQueueClusterOrchestrator.prototype.healthCheckJournal = function(clusterId,journalId,data,callback){
    var self = this
    log.dbg("Checking [cluster:"+clusterId+"] [journal:"+journalId+"] [data:"+log.pretty(data)+"]")
    this.checkClientConnection(this.createJournalClientFunction,data,function(err,client){
        if(client){
            client.shutdown()
        }
        data["start_date"] = new Date()
        self.updateClientStatus(clusterId,"journals",journalId,data,err,function(){
            if(err){
                var keys = Object.keys(self.clusters[clusterId]["nodes"]).filter(function(nodeId){
                    var nodeData = self.clusters[clusterId]["nodes"][nodeId]
                    if(nodeData.journals && nodeData.journals.indexOf(journalId) != -1){
                        return nodeId
                    }
                })
                for(var i in keys){
                    var nodeId = keys[i]
                    var node = self.clusters[clusterId]["nodes"][nodeId]
                    node.status = "DOWN"
                    self.updateClientStatus(clusterId,"nodes",nodeId,node,"Journal down",function(){})
                }
            }
            callback()
        })
    })
}

/**
 * Get the best journal for a node based on the start_date
 */
BigQueueClusterOrchestrator.prototype.getJournalForNode = function(clusterId,nodeId){
    var journals = this.clusters[clusterId]["nodes"][nodeId].journals
    var allJournals = this.clusters[clusterId]["journals"]
    var journalToReturn
    Object.keys(allJournals).forEach(function(j){
        var journal = allJournals[j]
        if(journals.indexOf(j) != -1){
            if(!journalToReturn){
                journalToReturn = journal
            }else{
                if(journalToReturn.data.start_time.getTime()>journal.data.start_time.getTime()){
                    journalToReturn = journal
                }
            }
        }
    })
    return journalToReturn
}


/**
 * This method will recover the messages from journal and
 * post this to the server
 */
BigQueueClusterOrchestrator.prototype.synchronizeMessages = function(clusterId,nodeId,client,callback){
    var self = this
    var journal = this.getJournalForNode(clusterId,nodeId)
    var journalClient = this.createJournalClientFunction(journal)
    log.inf("Recovering data from journal ["+log.pretty(journal)+"]")
    var journalError = false
    journalClient.on("error",function(err){
        journalError = true
        log.err("Error on journal: "+err)

    })
    journalClient.on("ready",function(){
        /*Errors should be global to all sync process to generate 
          a fail if a sync error is produced with any topic sync error*/
        var errors = []
        var synks = 0

        var recoverMessages = function(topic,head,topicsLength){
            if(head < 0)
                head = 1
            journalClient.retrieveMessages(nodeId,topic, head,function(err,messages){
                if(err || !messages || (messages && messages.length==0)){
                    callback(err)
                    return
                }
                var posted = 0
                for(var m in messages){
                    log.dbg("Recovering message ["+log.pretty(messages[m])+"]")
                    client.postMessage(topic,messages[m],function(err,id){
                        if(err){
                            errors.push(err)
                        }
                        posted++
                        if(posted == messages.length){
                            synks++
                        }
                        if(synks == topicsLength){
                            journalClient.shutdown()
                            if(errors.length > 0 || journalError){
                                if(journalError)
                                    console.log("Error on journal: ["+log.pretty(journal)+"]")
                                else
                                    callback(JSON.stringify(errors))
                            }else{
                                callback()
                            }
                            return
                        }
                    })
                }
            })

        }
        
        client.getHeads(function(err,heads){
            if(err){
                self.processingError(err)
                return
            }
            var topics = Object.keys(heads)
            log.inf("Recovering data for topics ["+log.pretty(topics)+"]")
            for(var i in topics){
                var topic = topics[i]
                var head = heads[topic]
                recoverMessages(topic,head,topics.length)
            }
        })
    })
}


/**
 * Check the status and re-sinchronize if it's needed an specifi node
 */
BigQueueClusterOrchestrator.prototype.healthCheckNode = function(clusterId,nodeId,data,callback){
    if(!this.running){
        callback()
        return
    }
    var self = this
    if(data.status == FORCEDOWN){
        callback()
        return
    }
    log.dbg("Checking [cluster:"+clusterId+"] [node:"+nodeId+"] [data:"+log.pretty(data)+"]")
    this.checkClientConnection(this.createNodeClientFunction,data,function(err,client){
        if(!err && client){
            self.checkDataStructure(client,clusterId,nodeId,data,function(err){
                if(err){
                    log.err("Error validating structure of ["+clusterId+"] ["+nodeId+"], error: "+log.pretty(err))
                    self.processingError(JSON.stringify(err))
                }
                if(data.journals){
                    var allUp = true
                    for(var i in data.journals){
                        var journalId = data.journals[i]
                        var journal = self.clusters[clusterId]["journals"][journalId]
                        allUp = allUp && (journal.status == "UP")
                    }
                    if(!allUp){
                        log.inf("Some journal down, putting node ["+clusterId+"] ["+nodeId+"] down")
                        err = "Journal down"
                    }
                }
                
                var updateStatus =  function(){
                    self.updateClientStatus(clusterId,"nodes",nodeId,data,err,function(){
                        client.shutdown()
                        callback()
                    })
                }

                /**
                 * If it has journals linked will recover the lost information
                 */
                if(!err && data.status!="UP" && data.journals && data.journals.length > 0){
                    self.synchronizeMessages(clusterId,nodeId,client,function(err){
                        if(err){
                            client.shutdown()
                            log.err("Error synchronizing messages, "+err)
                            callback("Error synchronizing message, "+err)
                            return;
                        }else{
                            updateStatus()
                        }
                    })
                }else{
                    updateStatus()
                }
            })
        }else{
            self.updateClientStatus(clusterId,"nodes",nodeId,data,err,function(){
                callback()
            })
        }
    })

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
        var journalKeys = Object.keys(cluster["journals"])
        for(var j in journalKeys){
            var journalKey = journalKeys[j]
            context.healthCheckJournal(clusterKey,journalKey,context.clusters[clusterKey]["journals"][journalKey],function(err){
            })
        }

        var nodesKeys = Object.keys(cluster["nodes"])
        for(var j in nodesKeys){
            var nodeKey = nodesKeys[j]
            context.healthCheckNode(clusterKey,nodeKey,context.clusters[clusterKey]["nodes"][nodeKey],function(err){
            })
        }
    }
}

BigQueueClusterOrchestrator.prototype.processingError = function(error){
    console.trace(error)
    console.log("Error processing")
    process.exit(1)
}
BigQueueClusterOrchestrator.prototype.shutdown = function(){
    this.running = false
    clearInterval(this.intervalId)
    this.nodeMonitor.running = false
    if(this.zkClient){
        this.zkClient.close()
    }
}
BigQueueClusterOrchestrator.prototype.init = function(){
    var self = this
    this.zkClient.connect(function(err){
        if(err){
            log.err("Error connecting to zookeeper ["+err+"]")
            self.emit("error",err)
            return
        }
        self.nodeMonitor.pathMonitor(self.zkClustersPath)
        setTimeout(function(){
            self.intervalId = setInterval(function(){
                self.nodeMonitor.refresh()
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
