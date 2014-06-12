#!/usr/local/bin/node 

var bq = require("../lib/bq_client.js"),
    log = require("../lib/bq_logger.js");
var externalConfig = process.argv[2]

var config 
if(externalConfig){
    config = require(externalConfig).orchestratorConfig
}else{
    config = {host:"127.0.0.1",port:6379}
}
log.setLevel(config.logLevel || "error");

var bqClient = bq.createClient(config)

bqClient.on("error", function(err) {
  console.error("An error, ocurrs", JSON.stringify(err));
  process.exit(1);
}) 

bqClient.on("ready",function(){
  bqClient.getNodeStats(function(err, data) {
    if(err) {
      bqClient.emit("error",err);
    } else {
      console.log(JSON.stringify(data));
      process.exit(0);
    }
  });
});
