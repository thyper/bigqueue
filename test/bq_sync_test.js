var nock = require("nock"),
    should = require("should"),
    redis = require("redis"),
    bq = require("../lib/bq_client.js"),
    bqj = require("../lib/bq_journal_client_redis.js"),
    bqSync = require("../lib/bq_sync.js");

describe("Sync tests", function() {
  
  var redisConf= {host:"127.0.0.1",port:6379}
  var redisClient;
  var nodeClient;
  var journalClient;
  before(function(done) {
    nodeClient = bq.createClient(redisConf);
    nodeClient.on("ready", function() {
      done();
    });
  });
  before(function(done) {
    journalClient = bqj.createJournalClient(redisConf);
    journalClient.on("ready", function() {
      done();
    });
  });
  before(function(done) {
    redisClient = redis.createClient(redisConf.port,redisConf.host)
    redisClient.on("ready",function(){
      done();
    });
  });
  beforeEach(function(done) {
    redisClient.flushall(function(err){
      done();
    });
  });

  describe("Structure", function() {
    it("It should create topics and consumers existent into db and unexistent in the node", function(done) {
      var clusterData = {
        cluster : "test",
        topics: [
          {
            topic_id: "topic1" ,
            ttl: 3600,
              consumers : [
                {
                  consumer_id: "consumer1",
                  stats : {}
                },
                {
                  consumer_id: "consumer2",
                  stats : {}
                }  
              ]
          },
          {
            topic_id: "topic2",
            ttl: 3600,
            consumers : [
              {
                consumer_id: "consumer1", 
                stats : {}
              }
            ]
          }
        ]
      };
      var bqClient = nodeClient;
       bqClient.createTopic("topic1", function(err) {
        should.not.exist(err);
         bqClient.createConsumerGroup("topic1", "consumer1", function(err) {
          should.not.exist(err);
          new bqSync().syncStructure(clusterData, bqClient, function(err) {
            should.not.exist(err);
            bqClient.listTopics(function(data) {
              data.should.include("topic1");
              data.should.include("topic2");
              bqClient.getConsumerGroups("topic1", function(err,data) {
                data.should.include("consumer1");
                data.should.include("consumer2");
                bqClient.getConsumerGroups("topic2", function(err,data) {
                  data.should.include("consumer1");
                  done();
                });
              });
            });
          });
        });
      });
    });
  });
  describe("Messages", function() {
    beforeEach(function(done) {
      nodeClient.createTopic("topic1", function(err) {
        should.not.exist(err);
        nodeClient.createTopic("topic2", function(err) {
          should.not.exist(err);
          nodeClient.createConsumerGroup("topic1", "t1-c1", function(err) {
            should.not.exist(err);
            nodeClient.createConsumerGroup("topic1", "t1-c2", function(err) {
              should.not.exist(err);
              nodeClient.createConsumerGroup("topic2", "t2-c1", function(err) {
                should.not.exist(err);
                nodeClient.createConsumerGroup("topic2", "t2-c2", function(err) {
                  should.not.exist(err);
                  done();
                });
              });
            });
          });
        });
      });

    });
    it("It should recover lost messages", function(done) {
      var messages = 5;
      var ready = 0;
      function test() {
        new bqSync().syncMessages("test", nodeClient, journalClient ,function(err) {
          should.not.exist(err);
          nodeClient.getHeads(function(err, heads) {
            parseInt(heads.topic1).should.equal(3);
            parseInt(heads.topic2).should.equal(2);
            done();
          });
        });
      }
      function onJournalReady(err) {
        should.not.exist(err);
        ready++;
        if(messages == ready) {
          test();
        }
      }
      journalClient.write("test", "topic1", 1, {msg: "test"}, 3600, onJournalReady);
      journalClient.write("test", "topic1", 2, {msg: "test"}, 3600, onJournalReady);
      journalClient.write("test", "topic1", 3, {msg: "test"}, 3600, onJournalReady);
      journalClient.write("test", "topic2", 1, {msg: "test"}, 3600, onJournalReady);
      journalClient.write("test", "topic2", 2, {msg: "test"}, 3600, onJournalReady);
    });
    it("It should only sink messages that doesn't exist", function(done) {
      var messages = 9;
      var ready = 0;
      function test() {
        new bqSync().syncMessages("test", nodeClient, journalClient ,function(err) {
          should.not.exist(err);
          nodeClient.getHeads(function(err, heads) {
            parseInt(heads.topic1).should.equal(3);
            parseInt(heads.topic2).should.equal(2);
            done();
          });
        });
      }
      function onJournalReady(err) {
        should.not.exist(err);
        ready++;
        if(messages == ready) {
          test();
        }
      }
      nodeClient.postMessage("topic1",{msg:"test"}, onJournalReady);
      nodeClient.postMessage("topic1",{msg:"test"}, onJournalReady);
      nodeClient.postMessage("topic1",{msg:"test"}, onJournalReady);
      nodeClient.postMessage("topic2",{msg:"test"}, onJournalReady);
      journalClient.write("test", "topic1", 1, {msg: "test"}, 3600, onJournalReady);
      journalClient.write("test", "topic1", 2, {msg: "test"}, 3600, onJournalReady);
      journalClient.write("test", "topic1", 3, {msg: "test"}, 3600, onJournalReady);
      journalClient.write("test", "topic2", 1, {msg: "test"}, 3600, onJournalReady);
      journalClient.write("test", "topic2", 2, {msg: "test"}, 3600, onJournalReady);

    });
  });
});
