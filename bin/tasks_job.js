var TasksJob = require("../lib/bq_tasks_job.js"),
    log = require("../lib/bq_logger.js");

var config
if(process.argv.length != 3){
    config = require(process.argv[2]).tasksJobConfig
}else{
  console.log("Config and type of job ('onetime', 'daemon') should be sent as parameter");
  process.exit(1);
}

log.setLevel(config.logLevel || "error");

var job = new TasksJob(config); 
var type = process.argv[3];

console.log("Starting tasks job");

function end(err) {
  if(err) {
    console.log("Error running tasks");
    console.log(err);
    process.exit(101);
  }
  console.log("Tasks finished");
  process.exit(0);
}

if(type == "ontime") {
  job.doProcess(end);
} else if(type == "daemon") {
  job.doMonitor(end);
} else {
  console.log("Job type not supported");
}

