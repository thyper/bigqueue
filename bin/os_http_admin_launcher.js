#!/usr/local/bin/node

/**
 * Executable to create the http api client
 * If no config found a default config will be used, this confing
 * will use a redis localhost running at default port
 */
var bq = require('../lib/bq_client.js'),
    ZK = require('zookeeper'),
    bqc = require('../lib/bq_cluster_client.js'),
    adm_api = require("../ext/openstack/bq_os_admin_http_api.js"),
    bj = require('../lib/bq_journal_client_redis.js')

var cluster = require('cluster');
var http = require('http');
var numCPUs = require('os').cpus().length;
var externalConfig = process.argv[2]

var bqPath = "/bq"

var zkConfig = {
    connect: "localhost:2181",
    timeout: 200000,
    debug_level: ZK.ZOO_LOG_LEVEL_WARN,
    host_order_deterministic: false
}   

var admConfig = {
    "zkConfig":zkConfig,
    "zkBqPath":bqPath,
    "createNodeClientFunction":bq.createClient,
    "createJournalClientFunction":bj.createJournalClient,
    "logLevel":"error",
    "defaultCluster":"default"
}

var httpConfig = {
    "admConfig":admConfig,
    "port":8080,
    "basePath":"/bigqueue",
    "logLevel":"critical",
    "loggerConf":{
         format:"[:date]\t[:method]\t[:url]\t[:status]\t[:remote-addr]\t[HTTP/:http-version]\t:req[content-type]\t:res[content-type]\t[:response-time]",
     }
}



//Check for external config
var config 
if(externalConfig){
    config = require(externalConfig).httpApiConfig
}else{
    config = httpConfig
}

//Run config
console.log("Using config: "+JSON.stringify(config))

api = adm_api.startup(config)

console.log("Admin api started")

