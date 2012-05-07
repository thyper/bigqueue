var redis = require('redis'),
    fs = require('fs'),
    should = require('should'),
    events = require("events"),
    ZK = require ("zookeeper"),
    ZKMonitor = require("../lib/zk_monitor.js"),
    os = require("os"),
    log = require("node-logging")

var topicsPath = "/topics"
var consumerGroupsPath = "/consumerGroups"

/**
 *  Object responsible for execute all commands throght the bq cluster
 */
function BigQueueClusterClient(zkClient,zkClusterPath,zkFlags,createClientFunction,createJournalFunction,sentCacheTime){
    this.nodes=[]
    this.journals=[]

    this.zkClient = zkClient
    this.zkClusterPath = zkClusterPath
    this.zkFlags = 0 | zkFlags
    this.createClientFunction = createClientFunction
    this.createJournalFunction = createJournalFunction
    this.loading = true
    this.nodesPath = this.zkClusterPath+"/nodes"
    this.journalsPath = this.zkClusterPath+"/journals"
    this.sentCacheTime = 60 || sentCacheTime
    this.hostname = os.hostname()
    this.uid = 0
    this.sentCache = {}
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
    this.zkClient.connect(function(err){
        if(err){
            log.err("Error connecting to zookeeper ["+log.pretty(err)+"]",true)
            self.emit("error",err)
            return
        }
        self.nodeMonitor = new ZKMonitor(self.zkClient,self)
        self.nodeMonitor.on("error",function(err){
            self.emit("error",err)
        })
        self.nodeMonitor.pathMonitor(self.nodesPath)
        self.nodeMonitor.pathMonitor(self.journalsPath)
        //Check if no nodes
        self.zkClient.a_get_children(self.nodesPath,false,function(rc,error,childrens){
            if(rc == 0 && childrens.length == 0){
                self.callReadyIfFinish()
            }
        })
    })
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
    this.nodeMonitor.running = false
    if(this.zkClient)
        this.zkClient.close()
}
/**
 * When node is added we0ll create a clusterNode object and if is loading we will notify this load
 */
BigQueueClusterClient.prototype.nodeAdded = function(node){
    var self = this
    var clusterNode = {}
    clusterNode["id"] = node.node
    clusterNode["data"] = JSON.parse(node.data)
    clusterNode["sentMessages"] = {}
    
    var meta = this.clientsMeta[node.path] 
    clusterNode["client"] = meta.createClient(clusterNode.data)
       
    clusterNode.client.on("end",function(){
        self.incrementClientErrors(clusterNode)
    })
    
    clusterNode.client.on("error",function(){
        if(clusterNode.data.status!="DOWN")
            self.incrementClientErrors(node.path,clusterNode)
    })
    
    this[meta.clientsList].push(clusterNode)

    if(this.loading)
        this.callReadyIfFinish()
    log.inf("Zookeeper node added ["+log.pretty(node)+"]")

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
    if(this.loaded == undefined)
        this.loaded = 0
    this.loaded++;
    if(this.loaded >= (this.nodeMonitor.monitoredPaths(this.nodesPath).length + 
        this.nodeMonitor.monitoredPaths(this.journalsPath).length)) {
       this.loading = false
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
    var oldData = clusterNode.data
    clusterNode.data = newData
    log.inf("Zookeeper node data chaged ["+log.pretty(node)+"]")
}

BigQueueClusterClient.prototype.incrementClientErrors = function(path,node){
    var self = this
    if(node.data.status == "UP"){
        node.data.errors++
        var nodePath = path+"/"+node.id
        this.zkClient.a_set(nodePath,JSON.stringify(node.data),-1,function(rc,error,stat){
            if(rc!=0){
                log.err("Error updating errors of ["+nodePath+"] in zookeeper")
            }
        })    
    }

}

/**
 * If an error node found we'll notify it to zookeeper
 */
BigQueueClusterClient.prototype.nodeError = function(clusterNode){
    this.incrementClientErrors(this.nodesPath,clusterNode)
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
            errors.push(err)
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
    var monitor = function(err,data){
        calls++
        if(!err){
            cb(err,data)
            return
        }
        if(calls < self.nodes.length){
            self.nodeError(actualClient)
            actualClient = self.nextNodeClient()
            run(actualClient,monitor)
        }else{
            var error = err
            cb({err:"Node execution fail ["+error+"]"},null)
        }
    }
    if(this.nodes.length == 0){
        cb({err:"No nodes found"},null)
        return
    }
    actualClient = this.nextNodeClient()
    run(actualClient,monitor)
}

/**
 * Creates a topic into all nodes
 */
BigQueueClusterClient.prototype.createTopic = function(){
    var topic = arguments[0]
    var callback
    var ttl
    if(arguments.length == 2){
        callback = arguments[1]
    }else{
        ttl = arguments[1]
        callback = arguments[2]
    }

    if(this.getNodesNoUp().length > 0){
        callback({err:"There are nodes down, try in few minutes"})
        return
    }

    var self = this
    var topicPath = this.zkClusterPath+topicsPath+"/"+topic
    var topicConsumerPath = this.zkClusterPath+topicsPath+"/"+topic+"/consumerGroups"
    //Check if exist in zookeeper
    this.zkClient.a_exists(topicPath,false,function(rc,error,stats){
            if(rc == ZK.ZNONODE){
                //If not exist create topic on each node
                self.withEvery(self.nodes,function(clusterNode,monitor){
                    clusterNode.client.createTopic(topic,ttl,function(err){
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
                        log.err("Error creating topic ["+log.pretty(err)+"]")
                        callback(err)
                        return
                    }
                    self.zkClient.a_create(topicPath,"",self.zkFlags,function(rc, error, path){
                        if(rc!=0){
                            log.err("Creating path in zookeeper ["+topicPath+"],error: "+error)
                            callback("Error registering topic ["+topic+"] into zookeeper")
                            return
                        }
                        self.zkClient.a_create(topicConsumerPath,"",self.zkFlags,function(rc, error, path){
                            if(rc!=0){
                                log.err("Creating path in zookeeper ["+topicConsumerPath+"], error: "+log.pretty(error))
                                callback("Error registering topic ["+topic+"] into zookeeper")
                                return
                            }
                            callback()
                        })
                    })
                })
            }else{
                //If already exist throw error
                callback({err:"Error topic ["+topic+"] already exist"})
            }
    })
}

/**
 * Creates a consumer group into all nodes
 */
BigQueueClusterClient.prototype.createConsumerGroup = function(topic,consumer,callback){
    if(this.getNodesNoUp().length > 0){
        callback({err:"There are nodes down, try in few minutes"})
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
                        callback(err)
                        return
                    }
                    self.zkClient.a_create(consumerPath,"",self.zkFlags,function(rc, error, path){
                        if(rc!=0){
                            log.error("Creating path in zookeeper ["+consumerPath+"], error: "+log.pretty(error))
                            callback({err: "Error registering consumer group ["+consumer+"] for topic ["+topic+"] into zookeeper"})
                            return
                        }
                        callback()
                    })
                })
            }else{
                //If already exist throw error
                callback({err:"Error consumer group ["+consumer+"] for topic ["+topic+"] already exist"})
            }
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
    var timer = log.startTimer()
    var self = this
    var uid = this.generateClientUID()
    message["uid"] = uid
    this.withSomeClient(
        function(clusterNode,monitor){
                if(!clusterNode || clusterNode.data.status != "UP"){
                    monitor("Error with clusterNode",null)
                    return
                }
                clusterNode.client.postMessage(topic,message,function(err,key){
                if(!err){
                    //If no error, write the data to the journals and load it to the local cache
                    key["uid"] = uid
                    //Load to journal
                    self.writeMessageToJournal(topic,key.id,message,clusterNode,function(err){
                        monitor(err,key)
                    })
                    
                }else{
                    monitor(err,key)
                    log.err("Error posting message ["+log.pretty(err)+"] ["+log.pretty(clusterNode.data)+"]")
                }
            })
        },
        function(err,key){
            timer("Post message to cluster")
            callback(err,key)
            timer("Posted cluster callback")
        }
   )
}

BigQueueClusterClient.prototype.writeMessageToJournal = function(topic, msgId, message, clusterNode, cb){
    var self = this
    if(!clusterNode.data.journals || clusterNode.data.journals.length == 0 ){
        cb(err,key)
    }else{
        var journalIds = clusterNode.data.journals
        var journals = []
        for(var i in journalIds){
            var journal = this.getJournalById(journalIds[i])
            if(!journal){
                var err = "Error getting journal ["+journalIds[i]+"] warranties not gotten" 
                log.err(err)
                cb(err,undefined)
                //It's a node error because it has declared an unexistent journal
                this.nodeError(clusterNode)
                return
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
    var timer = log.startTimer()
    this.withSomeClient(
        function(clusterNode,monitor){     
            if(!clusterNode || clusterNode.data.status != "UP"){
                monitor({err:"Cluster node error or down"})
                return
            }
            clusterNode.client.getMessage(topic,group,visibilityWindow,function(err,data){
                if(err){
                    monitor(err,undefined)
                }else{
                    if(data.id){
                        var recipientCallback = self.encodeRecipientCallback({"nodeId":clusterNode.id,"topic":topic,"consumerGroup":group,"id":data.id})
                        data["recipientCallback"] = recipientCallback
                        monitor(undefined,data)
                    }else{
                        monitor(undefined,undefined)
                    }
                }
            })
        },
        function(err,data){
            timer("Get message from cluster")
            if(data == {} || err)
                data = undefined
            if(err)
                log.err("Error getting messages ["+log.pretty(err)+"]")
            callback(err,data)
            timer("Getted Message from cluster Callback end")
        }
    )
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
    var repData = this.decodeRecipientCallback(recipientCallback)
    var node = this.getNodeById(repData.nodeId)
    if(!node || node.data.status != "UP"){
        callback({err:"Node ["+repData.nodeId+"] not found or down"})
        return
    }
    node.client.ackMessage(topic,group,repData.id,function(err){
        if(err)
            log.err("Error doing ack of recipientCallback ["+recipientCallback+"], error: "+log.pretty(err))
        callback(err)
    })
}

BigQueueClusterClient.prototype.failMessage = function(topic,group,recipientCallback,callback){
    var repData = this.decodeRecipientCallback(recipientCallback)
    var node = this.getNodeById(repData.nodeId)
    if(!node || node.data.status != "UP"){
        callback({err:"Node ["+repData.nodeId+"] not found or down"})
        return
    }
    node.client.failMessage(topic,group,repData.id,function(err){
        if(err)
            log.err("Error doing fail of recipientCallback ["+recipientCallback+"], error: "+log.pretty(err))
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
    var consumerGroupsPath = this.zkClusterPath+"/topics/"+topic+"/consumerGroups"
    this.zkClient.a_get_children(consumerGroupsPath,false,function(rc,error,children){
        if(rc!=0){
            log.err("Error getting consumer groups ["+log.pretty(error)+"]")
            callback({err:"Error ["+rc+"-"+error+", "+consumerGroupsPath+"]"},undefined)
            return
        }
        callback(undefined,children)
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
                callback(JSON.stingify(err),undefined)
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
                if(!clusterNode || clusterNode.data.status != "UP"){
                    monitor("Error with clusterNode",null)
                    return
                }
                clusterNode.client.getTopicTtl(topic,function(err,data){
                if(err){
                    monitor(err,undefined)
                }else{
                    monitor(err,data)
                }
            })
        },
        function(err,data){
            callback(err,data)
        }
   )

}

exports.createClusterClient = function(clusterConfig){
    var zk = new ZK(clusterConfig.zkConfig)
    return  new BigQueueClusterClient(zk,clusterConfig.zkClusterPath,
                                            clusterConfig.zkFlags,
                                            clusterConfig.createNodeClientFunction,
                                            clusterConfig.createJournalClientFunction,
                                            clusterConfig.sentCacheTime)
} 
