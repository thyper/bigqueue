
function BigQueueSync(config) {
  this.config = config;
}

BigQueueSync.prototype.getNodeData = function(client,callback){
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

BigQueueSync.prototype.createUnexistentTopics = function(clusterData, client,nodeData,callback){
    var self = this
    var nodeTopics = Object.keys(nodeData)
    var unexistent = clusterData.topics.filter(function(val){
        return nodeTopics.indexOf(val.topic_id) != 0;
    })
    var opsCount = unexistent.length
    var opsExec = 0
    var finished = false

    if(unexistent.length == 0){
        callback()
        return
    }
    unexistent.forEach(function(topic){
      client.createTopic(topic.topic_id,topic.ttl,function(err,data){
          opsExec++
          if(opsExec >= opsCount && !finished){
              finished = true
              callback()
          }
      })
    })
}

BigQueueSync.prototype.createUnexistentConsumerGroups = function(clusterData, client,nodeData,callback){
    var consumerGroupsToCreate = []
    var allTopics = clusterData.topics;
    clusterData.topics.forEach(function(topic) {
      topic.consumers.forEach(function(consumer) {
        if(!nodeData[topic.topic_id] || nodeData[topic.topic_id].indexOf(consumer.consumer_id) != 0) {
          consumerGroupsToCreate.push({topic_id: topic.topic_id, consumer_id: consumer.consumer_id});
        }
      });
    });
    var opsCount = consumerGroupsToCreate.length
    var opsExec = 0
    var finished = false
    if(opsCount == 0){
        callback()
        return
    }
    consumerGroupsToCreate.forEach(function(toCreate){
      client.createConsumerGroup(toCreate.topic_id,toCreate.consumer_id,function(err,data){
          if(err) {
            callback(err);
          } else {
           opsExec++
          }
          if(opsExec >= opsCount && !finished){
              callback()
              finished = true
          }
      })
    })
}


/**
 * It will be tasked to create all topics or consumers that actually exists on 
 * the DB but are not created in the node, It's very usefull when new nodes are joined to the cluster
 */
BigQueueSync.prototype.syncStructure = function(clusterData, nodeClient, callback) {
  var self = this;
  self.getNodeData(nodeClient,function(err,nodeData){
    if(err){
        return callback(err)
    }
    self.createUnexistentTopics(clusterData, nodeClient, nodeData,function(err){
      if(err){
          callback(err)
          return
      }
      self.createUnexistentConsumerGroups(clusterData, nodeClient, nodeData,function(err){
          if(err){
              callback(err)
              return
          }
          callback()
      })
    })
  })
}

BigQueueSync.prototype.syncMessages = function(nodeId, nodeClient, journalClient, callback) {
  /*Errors should be global to all sync process to generate 
    a fail if a sync error is produced with any topic sync error*/
  var errors = []
  var synks = 0
  function recoverMessages(topic,head,topicsLength) {
      if(head < 0)
          head = 1
      console.log("Recovering message for "+topic+", head: "+head);
      journalClient.retrieveMessages(nodeId,topic, head,function(err,messages){
        console.log(messages)
          if(err || !messages || (messages && messages.length==0)){
              callback(err)
              return
          }
          var posted = 0
          for(var m in messages){
              nodeClient.postMessage(topic,messages[m],function(err,id){
                  if(err){
                      errors.push(err)
                  }
                  posted++
                  if(posted == messages.length){
                      synks++
                  }
                  if(synks == topicsLength){
                      if(errors.length > 0){
                              callback(JSON.stringify(errors));
                      }else{
                          callback();
                      }
                      return;
                  }
              })
          }
      })

  }
  nodeClient.getHeads(function(err,heads){
      if(err){
        return callback(err);
      }
      console.log(heads)
      var topics = Object.keys(heads)
      if(topics.length == 0)
          callback()
      console.log("Recovering data for topics ["+JSON.stringify(topics)+"]")
      for(var i in topics){
          var topic = topics[i]
          var head = heads[topic]
          recoverMessages(topic,parseInt(head)+1,topics.length)
      }
  })
}

module.exports = BigQueueSync
