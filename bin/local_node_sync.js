var bq = require("../lib/bq_client.js"),
    bqj = require("../lib/bq_journal_client_redis.js"),
    sync = require("../lib/bq_sync.js"),
    log = require("node-logging");

var config
if(process.argv.length != 2){
    config = require(process.argv[2]).syncConfig
}else{
  console.log("Config and type of sync ('full', 'structure') should be sent as parameter");
  process.exit(1);
}

var syncProcess;
var full;
var syncObj = new sync(config);
if(process.argv[3] == "full") {
  full = true;
} else if (process.argv[3] == "structure") {
  full = false;
} else {
  console.log(process.arv[2]+" is not a valid sync type, it should be ('full' or 'structure')")
  process.exit(1);
}

console.log("Starting sync");

syncObj.syncProcess(full, function(err) {
  if(err) {
    console.log("Error running sync");
    console.log(err);
    process.exit(101);
  }
  console.log("Sync finished");
  process.exit(0);
});

