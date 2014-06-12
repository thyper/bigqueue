var bq = require("../lib/bq_client.js"),
    bqj = require("../lib/bq_journal_client_redis.js"),
    sync = require("../lib/bq_sync.js"),
    log = require("../lib/bq_logger.js");

var config
if(process.argv.length != 2){
    config = require(process.argv[2]).syncConfig
}else{
  console.log("Config and type of sync ('full', 'structure', 'messages') should be sent as parameter");
  process.exit(1);
}

var syncProcess;
var full;
var syncObj = new sync(config);

log.setLevel(config.logLevel || "error");
console.log("Starting sync");

syncObj.syncProcess(process.argv[3], function(err) {
  if(err) {
    console.log("Error running sync");
    console.log(err);
    process.exit(101);
  }
  console.log("Sync finished");
  process.exit(0);
});

