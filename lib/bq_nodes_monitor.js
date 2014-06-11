var needle = require('needle'),
    log = require("winston");

function NodesMonitor(config) {
  this.adminApi = config.adminApi;
  this.refreshInterval = config.refreshInterval;
  this.cluster = config.cluster;
  this.clusterUrl = this.adminApi+"/clusters/"+this.cluster;
  this.nodes = {};
  this.running = true; 
  this.startup();  
}

NodesMonitor.prototype = Object.create(require('events').EventEmitter.prototype);

NodesMonitor.prototype.refreshData = function(callback) {
  var self = this;
  log.log("info", "Checking: "+this.clusterUrl);
  needle.get(this.clusterUrl, function(err, response) {
  try {
    if(err) {
      log.log("error", err);
      if(callback) {
        callback(err);
      }
      throw err;
    }
    if(response.statusCode != 200) {
      var error = new Error("Error getting data from ["+this.clusterUrl+"] status ["+response.statusCode+"] ["+response.body+"]");
      log.log("error", error);
      throw error; 
    }
  } catch(e) {
    callback(e);
    try {
      this.emit("error",e);
    } catch(e) {}
    return log.log("info", "Error getting data from api",e);
  }
  log.log("info", "Check success");
  var local_nodes = {};
  if(response.body && response.body.nodes) {
    response.body.nodes.forEach(function(e) {
      if(e) {
        e["type"] = "node";
        var id = e.id;
        local_nodes[id] = e;
      }
    });
  }
  if(response.body && response.body.journals) {
    response.body.journals.forEach(function(e) {
      if(e) {
        e["type"] = "journal";
        var id = e.id;
        local_nodes[id] = e;
      }
    });
  }
  //Look for deleted nodes
  Object.keys(self.nodes).forEach(function(n) {
    if(!local_nodes[n]) {
      self.emit("nodeRemoved", self.nodes[n]);
    }
  });
  //Look for added or modified
  Object.keys(local_nodes).forEach(function(n) {
   if(!self.nodes[n]) {
    self.emit("nodeChanged", local_nodes[n]);
    self.nodes[n] = local_nodes[n];
   } else {
    var new_data = local_nodes[n];
    var old_data = self.nodes[n];
    var changes = Object.keys(new_data).some(function(prop) {
      var new_prop = new_data[prop];
      var old_prop = old_data[prop];
      if(!old_prop && new_prop) {
        return true;
      }
      if(new_prop instanceof Array && old_prop instanceof Array) {
        return new_prop.length != old_prop.length && new_prop.some(function(arr_e) {
          return old_prop.indexOf(arr_e) == -1;
        });
      } else {
        return new_prop != old_prop;
      }
      return true;
    });
    if(changes) {
      self.emit("nodeChanged", local_nodes[n]);
      self.nodes[n] = local_nodes[n];
    }
   } 
  });
  if(callback) {
    callback();
  }
 }); 
}

NodesMonitor.prototype.startup = function() {
  var self = this;
  this.setMaxListeners(5000);
  function run() {
    self.refreshData(function() {
      if(self.running) {
        setTimeout(run, self.refreshInterval);
      }
    });
  }
  run();
};

NodesMonitor.prototype.shutdown = function() {
  this.running = false;
}

module.exports = NodesMonitor;
