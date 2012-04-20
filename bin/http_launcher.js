#!/usr/local/bin/node

/**
 * Executable to create the http api client
 * If no config found a default config will be used, this confing
 * will use a redis localhost running at default port
 */
var bq = require('../lib/bq_client.js'),
    ZK = require('zookeeper'),
    bqc = require('../lib/bq_cluster_client.js'),
    http_api = require("../http_api.js")

var cluster = require('cluster');
var http = require('http');
var numCPUs = require('os').cpus().length;
var externalConfig = process.argv[2]

//Default redis conf
var redisLocalhost = {
    host:"127.0.0.1",
    port:6379,
    "options":{"command_queue_high_water":5000,"command_queue_low_water":1000}
}

//Default api conf
var httpApiConfig = {
    "port": 8080,
    "bqConfig": redisLocalhost, 
    "bqClientCreateFunction": bq.createClient,
    "logLevel":"info"
}

//Check for external config
var config 
if(externalConfig){
    config = require(externalConfig).httpApiConfig
}else{
    config = httpApiConfig
}

//Run config
console.log("Using config: "+JSON.stringify(config))
if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('death', function(worker) {
    console.log('worker ' + worker.pid + ' died');
    cluster.fork()
  });
} else {
    http_api.startup(config)
}

