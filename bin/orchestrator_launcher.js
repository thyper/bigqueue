#!/usr/local/bin/node 

var ZK = require("zookeeper"),
    oc = require("../lib/bq_cluster_orchestrator.js"),
    bq = require("../lib/bq_client.js"),
    bj = require("../lib/bq_journal_client_redis.js")

var externalConfig = process.argv[2]

var zkConfig = {
        connect: "localhost:2181",
        timeout: 200000,
        debug_level: ZK.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false
    }   

var ocConfig = {
    "zkClustersPath":"/bq/clusters",
    "zkConfig":zkConfig,
    "createNodeClientFunction":bq.createClient,
    "createJournalClientFunction":bj.createJournalClient,
    "checkInterval":2000,
    "restartOnSlowRunning":3
}

var config 
if(externalConfig){
    config = require(externalConfig).orchestratorConfig
}else{
    config = ocConfig 
}
var runningSlow = 0
var orch
var startOrchestrator = function(){
    orch = oc.createOrchestrator(config)
    orch.on("ready",function(){
        console.log("Orchestrator running")
    })
    if(config.restartOnSlowRunning != undefined && config.restartOnSlowRunning>0){ 
        orch.on("running-slow",function(){
            runningSlow++
            if(runningSlow >= config.restartOnSlowRunning){
                console.log("Restarting orchestrator because is too slow")
                orch.shutdown()
                runningSlow=0
                startOrchestrator()
            }
        })
        orch.on("error",function(err){
            console.log("error on orchestrator exiting ["+error+"]")
            process.exit(1)
        })
    }
    orch.on("running-start",function(){
        runningSlow = 0 
    })
}

startOrchestrator()
