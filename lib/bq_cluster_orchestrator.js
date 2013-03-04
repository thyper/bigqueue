var events = require("events"),
    ZK = require("zookeeper"),
    ZKMonitor = require("../lib/zk_monitor.js"),
    log = require("node-logging"),
    os = require("os"),
    utils = require("../lib/bq_client_utils.js");
var ADDED = "ADDED"
var REMOVED = "REMOVED"
var DATACHANGE = "DATACHANGE"
var FORCEDOWN = "FORCEDOWN"
var MY_HOST=os.hostname()
function BigQueueClusterOrchestrator(zkClient,zkClustersPath,createNodeClientFunction,createJournalClientFunction,checkInterval,amountOfOrchestratorToSetDownANode){
    this.zkClient = zkClient
    this.zkClustersPath = zkClustersPath
    this.createNodeClientFunction = createNodeClientFunction
    this.createJournalClientFunction = createJournalClientFunction
    this.checkInterval = checkInterval
    this.cluster = {}
    this.running = true
    this.healthCheckRunning = false
    this.amountOfOrchestratorToSetDownANode = amountOfOrchestratorToSetDownANode || 3
    this.basePath = this.zkClustersPath.substr(0,this.zkClustersPath.lastIndexOf("/"))
    var self = this
    this.zkRules = [
        {regexPath: new RegExp(self.basePath+"$"), controller: self.clustersController},
        {regexPath: new RegExp(self.basePath+"/(\\w+)$"), controller: self.createMonitorPath},
        {regexPath: new RegExp(self.zkClustersPath+"/topics$"), controller: self.topicsController},
        {regexPath: new RegExp(self.zkClustersPath+"/topics/(.*)/consumerGroups$"), controller: self.consumerGroupsController},
        {regexPath: new RegExp(self.zkClustersPath+"/nodes$"), controller: self.nodesController},
        {regexPath: new RegExp(self.zkClustersPath+"/journals$"), controller: self.journalsController},
                
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
        context.cluster = {"data":node.data, topics:{}, nodes:{}, journals:{}}
        var clusterPath = node.path+"/"+node.node
        context.nodeMonitor.pathMonitor(clusterPath)
    }
}

BigQueueClusterOrchestrator.prototype.topicsController = function(node,type,rule,context){
    if(type == ADDED){
        var topicData = {}
        try{
            topicData = JSON.parse(node.data)
        }catch(e){}
        context.cluster["topics"][node.node] = {"data":topicData}
        context.cluster["topics"][node.node]["consumerGroups"] = []
        context.nodeMonitor.pathMonitor(node.path+"/"+node.node+"/consumerGroups")
    }else if(type == REMOVED){
        delete context.cluster["topics"][node.node]
    }else if(type == DATACHANGE){
        var topicData = {}
        try{
            topicData = JSON.parse(node.data)
        }catch(e){}
        if(!context.cluster["topics"]){
            this.topicsController(node,ADDED,rule,context)
        }else{
            context.cluster["topics"][node.node].data = topicData
        }
    }
}

BigQueueClusterOrchestrator.prototype.consumerGroupsController = function(node,type,rule,context){
    var pathData = node.path.match(/.*\/(.*)\/topics\/(.*)\/consumerGroups$/)
    var cluster = pathData[1]
    var topic = pathData[2]
    if(type == ADDED){
        if(context.cluster["topics"][topic]["consumerGroups"] == undefined){
            context.cluster["topics"][topic]["consumerGroups"] = []
        }
        context.cluster["topics"][topic]["consumerGroups"].push(node.node)
   }else if(type == REMOVED){
       var idx = context.cluster["topics"][topic]["consumerGroups"].indexOf(node.node)
       if(idx>-1) 
         context.cluster["topics"][topic]["consumerGroups"].splice(idx,1)
   }
}

BigQueueClusterOrchestrator.prototype.nodesController = function(node,type,rule,context){
    var data
    try{
        data = JSON.parse(node.data)
    }catch(e){
        log.err("Error parsing data for node ["+log.pretty(node)+"]: "+data)
        return
    }
    if(type == ADDED || type == DATACHANGE){
        if(!data.orchestrator_errors)
            data.orchestrator_errors = []

        context.cluster["nodes"][node.node] = data
    }else if(type == REMOVED){
        delete context.cluster["nodes"][node.node]
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
    if(type == ADDED || type == DATACHANGE){
        if(!data.orchestrator_errors)
            data.orchestrator_errors = []
        context.cluster["journals"][node.node] = data 
    }else if(type == REMOVED){
        delete context.cluster["journals"][node.node] 
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
BigQueueClusterOrchestrator.prototype.updateClientStatus = function(clientType,clientId,data,err,callback){
    var self = this
    var shouldUpdate = false
    /*
     * Before any run we'll cehck if the data is complete
     */
    if(clientType == "nodes" && !utils.checkNodeData(data)){
        return this.processingError("Error trying update data, data to be updated empty or uncomplete ["+clientType+"] ["+clientId+"] ["+JSON.stringify(data)+"]")
    }else if(clientType == "journals" && !utils.checkJournalData(data)){
        return this.processingError("Error trying update data, data to be updated empty or uncomplete ["+clientType+"] ["+clientId+"] ["+JSON.stringify(data)+"]")
    }
    /**
     * In case of error We'll add the node to the listo of orchestrators seeing error
     * else we'll remove it if exist in the list
     */
    if(err){
        if(data.orchestrator_errors.indexOf(MY_HOST) < 0){
            data.orchestrator_errors.push(MY_HOST)
            shouldUpdate=true
        }
    }else{
        var idx = data.orchestrator_errors.indexOf(MY_HOST)
        if(idx>=0){
            data.orchestrator_errors.splice(idx,1)
            shouldUpdate=true
        }
    }
    /**
     * if 
     *  errors 
     *  and dataStatus is not DOWN 
     *  and the amount of orchestrators to set a node down is reached
     * we'll set the node data DOWN
     *
     * By the other hand
     *
     * if
     *  no errors
     *  and the node is not UP
     *  and the amount of orchestrators seeing errors is not the amount enough to set a node DOWN
     * we'll set the node UP
     */ 
    if(err 
        && data.status!="DOWN"
        && data.orchestrator_errors.length >= this.amountOfOrchestratorToSetDownANode ){
            data.status="DOWN"
            shouldUpdate = true
    }else if(!err 
        && data.status!="UP"
        && data.orchestrator_errors.length < this.amountOfOrchestratorToSetDownANode ){
            data.status = "UP"
            data.errors = 0
            shouldUpdate = true
    }
    if(shouldUpdate){
            this.cluster[clientType][clientId] = data
            var zkPath = this.zkClustersPath+"/"+clientType+"/"+clientId
            self.zkClient.a_set(zkPath,JSON.stringify(data),-1,function(rc, error, stat){
                if(rc!=0){
                    log.err("Error setting status UP to journal  ["+clientId+"], error: "+error)
                    /*Check if zookeeper is not closing && The node has not been removed
                     *
                     * -116 = Error, connection closing
                     * -101 = Error, no node
                     *
                     */
                    if(rc != -116 && rc != -101){
                        self.processingError(error)
                        return
                    }else{
                        callback("Error, ["+data+"] has not been updated ["+rc+" - "+error+"]");
                    }
                }
                log.inf("State of "+clientType+" ["+clientId+"] changed to "+data.status)
                callback()
            })

    }else{
        callback()
    }
    
}

/**
 * Check the status of an specific journal
 */
BigQueueClusterOrchestrator.prototype.healthCheckJournal = function(journalId,data,callback){
    var self = this
    log.dbg("Checking [journal:"+journalId+"] [data:"+log.pretty(data)+"]")
    this.checkClientConnection(this.createJournalClientFunction,data,function(err,client){
        if(client){
            client.shutdown()
        }
        data["start_date"] = new Date()
        self.updateClientStatus("journals",journalId,data,err,function(){
            callback()
        })
    })
}

/**
 * Get the best journal for a node based on the start_date
 */
BigQueueClusterOrchestrator.prototype.getJournalForNode = function(nodeId){
    var journals = this.cluster["nodes"][nodeId].journals
    var allJournals = this.cluster["journals"]
    var journalToReturn
    Object.keys(allJournals).forEach(function(j){
        var journal = allJournals[j]
        if(journals.indexOf(j) != -1){
            if(!journalToReturn && journal.status == "UP"){
                journalToReturn = journal
            }else{
                if(journalToReturn.data.start_time.getTime()>journal.data.start_time.getTime() && jorunal.data.status == "UP"){
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
BigQueueClusterOrchestrator.prototype.synchronizeMessages = function(nodeId,client,callback){
    var self = this
    var journal = this.getJournalForNode(nodeId)
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
                                    log.err("Error on journal: ["+log.pretty(journal)+"]")
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
BigQueueClusterOrchestrator.prototype.healthCheckNode = function(nodeId,data,callback){
    if(!this.running){
        callback()
        return
    }
    var self = this
    if(data.status == FORCEDOWN){
        callback()
        return
    }
    var updateStatus =  function(client,err){
        self.updateClientStatus("nodes",nodeId,data,err,function(){
            if(client)
            client.shutdown()
            callback()
        })
    }
    log.dbg("Checking  [node:"+nodeId+"] [data:"+log.pretty(data)+"]")
    /*
     * Check client connection
     * */
    this.checkClientConnection(this.createNodeClientFunction,data,function(err,client){
        /*
         * If any error occurs we'll emit the notification error
         */
        if(!err && client){
            /*
             * If the connection can be stablish we'll check the topics structure
             */
            self.checkDataStructure(client,nodeId,data,function(err){
                if(err){
                    /* 
                     * If the connection can be stablish and an error ocurrs checking the structure
                     * we'll finish the orchestrator beacause is a fatal error
                     */
                    log.err("Error validating structure of ["+nodeId+"], error: "+log.pretty(err))
                    return self.processingError(JSON.stringify(err))
                }
                if(data.journals){
                    /*
                     * If the node contains journals we'll check that every journal is UP,
                     * if not we'll notify this error
                     */
                    var allUp = true
                    for(var i in data.journals){
                        var journalId = data.journals[i]
                        var journal = self.cluster["journals"][journalId]
                        allUp = allUp && (journal.status == "UP")
                    }
                    if(!allUp){
                        log.inf("Some journal down, putting node ["+nodeId+"] down")
                        err = "Journal down"
                        updateStatus(client,err)
                    }
                }
                

                /**
                 * If at this moment everything is ok, we'll check
                 *
                 *  - The node is not UP
                 *  - Contains journals
                 *  - the amount of orchestrator is more than or equals than the amount of orchestrators needed to put a node down
                 *  So it's time to recover messages from journals and update the node statatus 
                 */
                if(!err && 
                        data.status!="UP" && 
                        data.journals &&
                        data.journals.length > 0 ){
                    self.synchronizeMessages(nodeId,client,function(err){
                        if(err){
                            /*
                             * If we should sincronize and an error occurs 
                             * we'll return an error and no change on metadata will be emited
                             */ 
                            client.shutdown()
                            log.err("Error synchronizing messages, "+err)
                            return callback("Error synchronizing message, "+err)
                        }else{
                            //We've sinchronized all messages, it's time to notify it
                            updateStatus(client)
                        }
                    })
                }else{
                    updateStatus(client,err)
                }
            })
        }else{
            updateStatus(client,err)
        }
    })

}

/**
 * It will check any topics or consumers existent in zookeeper and unexistent into the node
 * If any difference is found this function will try to solve it. It's very usefull when a new node
 * is joined to the cluster
 */
BigQueueClusterOrchestrator.prototype.checkDataStructure = function(client,nodeId,data,callback){
    if(!this.running){
        callback()
        return
    }
    var clusterTopics = this.cluster["topics"]
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
        self.createUnexistentTopics(client,nodeData,function(err){
            if(err){
                callback(err)
                return
            }
            self.createUnexistentConsumerGroups(client,nodeData,function(err){
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

BigQueueClusterOrchestrator.prototype.createUnexistentTopics = function(client,nodeData,callback){
    var self = this
    var topics = Object.keys(this.cluster["topics"])
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
    unexistent.forEach(function(val){
        var ttl = self.cluster["topics"][val].data.ttl
        log.inf("Creating unexistent topic ["+val+"] for client ["+log.pretty(client)+"]")   
        client.createTopic(val,ttl,function(err,data){
            opsExec++
            nodeData[val]=[]
            if(opsExec >= opsCount && !finished){
                finished = true
                callback()
            }
        })
    })
}

BigQueueClusterOrchestrator.prototype.createUnexistentConsumerGroups = function(client,nodeData,callback){
    var consumerGroupsToCreate = [] 
    var allTopics = this.cluster["topics"]
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
        log.inf("Creating unexistent group ["+log.pretty(val)+"] for client ["+log.pretty(client)+"]")   
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
      var journalKeys = Object.keys(context.cluster["journals"])
      var nodesKeys = Object.keys(context.cluster["nodes"])
      var journalsChecked = 0
      var nodesChecked = 0
      var self = this
      var checkRunning = function(){
          if(journalsChecked == journalKeys.length && nodesChecked == nodesKeys.length){
              self.healthCheckRunning = false
              log.inf("HelathCheck finished")
          }
      }

      for(var j in journalKeys){
          var journalKey = journalKeys[j]
          context.healthCheckJournal(journalKey,context.cluster["journals"][journalKey],function(err){
                journalsChecked++
                checkRunning()
          })
      }

      for(var j in nodesKeys){
          var nodeKey = nodesKeys[j]
          context.healthCheckNode(nodeKey,context.cluster["nodes"][nodeKey],function(err){
                nodesChecked++
                checkRunning()
          })
      }
}

BigQueueClusterOrchestrator.prototype.processingError = function(error){
    
    log.err("Error processing")
    if(this.running){
        this.emit("error",error)
        log.err(error,true)
    }
    this.shutdown()
}
BigQueueClusterOrchestrator.prototype.shutdown = function(){
    this.running = false
    clearInterval(this.intervalId)
    this.nodeMonitor.running = false
    if(this.zkClient){
        this.zkClient.close()
    }
}

/**
 * This is the main function called on startup
 */
BigQueueClusterOrchestrator.prototype.init = function(){
    var self = this
    this.zkClient.connect(function(err){
        if(err){
            log.err("Error connecting to zookeeper ["+err+"]")
            self.emit("error",err)
            return
        }
        self.nodeMonitor.pathMonitor(self.basePath)
        //After an initial wait the check will be started
        setTimeout(function(){
            self.intervalId = setInterval(function(){
                if(self.healthCheckRunning){
                    log.inf("Orchestrator is already running the new check won't be launched")
                    self.emit("running-slow")
                    return
                }
                log.inf("Starting orchestrator healthCheck")
                self.emit("running-start")
                self.healthCheckRunning = true
                /*
                 * We call refresh to be secure that the last version of zookeeper 
                 * data will be in memory (It's check is to avoid any connection problem)
                 */
                self.nodeMonitor.refresh(function(){
                    /**
                     * Do the job
                     */
                    self.periodicalCheck(self)
                })
            },self.checkInterval)
        },self.checkInterval)
        self.emit("ready")
    })

}

exports.createOrchestrator = function(conf){
   var zk = new ZK(conf.zkConfig)
   log.setLevel(conf.logLevel || "info")
   var orch = new BigQueueClusterOrchestrator(zk,conf.zkClustersPath,conf.createNodeClientFunction,conf.createJournalClientFunction,conf.checkInterval,conf.amountOfOrchestratorToSetDownANode)
   return orch
}
