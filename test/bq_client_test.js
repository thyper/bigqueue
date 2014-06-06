var should = require('should'),
    redis = require('redis'),
    bq = require('../lib/bq_client.js'),
    log = require("node-logging")

describe("Big Queue Client",function(){
    
    var redisClient
    var redisConf= {host:"127.0.0.1",port:6379}
    var bqClient
    before(function(done){
        log.setLevel("critical")
        bqClient = bq.createClient(redisConf)
        bqClient.on("ready",function(){
            redisClient = redis.createClient(redisConf.port,redisConf.host)
            redisClient.execute = function() {
              var args = [];
              for (var key in arguments) {
                args.push(arguments[key]);
              }
              var command = args.shift();
              var callback = args.pop();
              return redisClient.send_command(command, args, callback);
            }
            redisClient.on("ready",function(){
                done()
            })
        })
    })    

    beforeEach(function(done){
        redisClient.execute("FLUSHALL",function(data,err){
            done()
        })
    })    

    describe("#createTopic",function(){
        it("should add the topic to the topic list",function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                redisClient.execute("SISMEMBER","topics","testTopic",function(err,data){
                   should.not.exist(err)
                   data.should.equal(1)
                   done()
                })
            })
        })
        it("should set a ttl if is set as parameter",function(done){
            bqClient.createTopic("testTopic",1,function(err){
                should.not.exist(err)
                redisClient.execute("GET","topics:testTopic:ttl",function(err,data){
                    should.not.exist(err)
                    data.toString().should.equal(""+1)
                    done()
                })
            })
        })
        it("should get an error if the topic already exist",function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                bqClient.createTopic("testTopic",function(err){
                    should.exist(err)
                    done()
                })
            })
        })
    }) 

    describe("#createConsumerGroup",function(){
        beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                done()
            })
        })
        it("should add the consumer to the consumer list",function(done){
            bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                should.not.exist(err)
                redisClient.execute("SISMEMBER","topics:testTopic:consumers","testConsumer",function(err,data){
                    should.not.exist(err)
                    data.should.equal(1)
                    done()
                })
            })
        })
        it("should add the consumer key 'last'",function(done){
            bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                should.not.exist(err)
                redisClient.execute("EXISTS","topics:testTopic:consumers:testConsumer:last",function(err,data){
                    should.not.exist(err)
                    data.should.equal(1)
                    done()
                })
            })
        })
        it("should get an error if the consumer group already exist",function(done){
            bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                should.not.exist(err)
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    should.exist(err)
                    done() 
                })
            })

        })
    })

    describe("#postMessage",function(){
         beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                done()
            })
        })

        it("should add the message to redis and return their key",function(done){
            bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,key){
                should.not.exist(err)
                key.id.should.be.above(0)
                redisClient.execute("EXISTS","topics:testTopic:messages:"+key.id,function(err,data){
                    should.not.exist(err)
                    data.should.equal(1)
                    done()
                })
            })
        })
        it("should return an error if topic doesn't exist",function(done){
            bqClient.postMessage("testTopic-noExist",{msg:"testMessage"},function(err,key){
                should.exist(err)
                done()
            })
        })
    })

    describe("#getMessage",function(){
         var generatedKey
         beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
                        generatedKey = key.id
                        should.not.exist(err)
                        done()
                    })
                })
            })
        })

        it("should get a message stored on redis",function(done){
            bqClient.getMessage("testTopic","testConsumer",undefined,function(err,data){
                should.not.exist(err)
                data.should.have.keys("msg","id","recipientCallback","remaining")
                data.remaining.should.equal(0);
                data.id.should.equal(""+generatedKey)
                done()
            })    
        });
       it("Remaining should show amount of message remaining",function(done){
          bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
            should.not.exist(err)
            bqClient.getMessage("testTopic","testConsumer",undefined,function(err,data){
              should.not.exist(err)
              data.remaining.should.equal(1);
              bqClient.getMessage("testTopic","testConsumer",undefined,function(err,data){
                should.not.exist(err)
                data.remaining.should.equal(0);
                done()
              }) 
            }) 
          })
        });
        it("should override visibility window if one is set as param",function(done){
            var tmsExpired = Math.floor(new Date().getTime()/1000)+10
            bqClient.getMessage("testTopic","testConsumer",10,function(err,data){
                redisClient.execute("zrangebyscore","topics:testTopic:consumers:testConsumer:processing","-inf","+inf","withscores",function(err,data){
                    should.not.exist(err)
                    data[1].should.be.above(tmsExpired-2)
                    data[1].should.be.below(tmsExpired+1)
                    done()
                })

            })
        })
        it("should get an error if consumer group doesn't exist",function(done){
            bqClient.getMessage("testTopic","testConsumer-noExist",undefined,function(err,data){
                should.exist(err)
                done()
            })
        })
    })

    describe("#ackMessage",function(){
         var id;
         beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
                        bqClient.getMessage("testTopic","testConsumer",undefined,function(err,msg){
                            should.not.exist(err)
                            should.exist(msg)
                            id = msg.recipientCallback
                            done()
                        })
                    })
                })
            })
        })

        it("should set a message as processed",function(done){
            bqClient.ackMessage("testTopic","testConsumer",id,function(err){
                should.not.exist(err)
                redisClient.execute("zrangebyscore","topics:testTopic:consumers:testConsumer:processing","-inf","+inf",function(err,data){
                    should.not.exist(err)
                    data.should.not.include(""+id)
                    done()
                })
            })    
        })
        it("should get an error if message can't be acked",function(done){
            bqClient.ackMessage("testTopic","testConsumer",15,function(err){
                should.exist(err)
                done()
            })
        })
        it("should get an error if consumer group doesn't exist",function(done){
            bqClient.ackMessage("testTopic","testConsumer-noExist",15,function(err){
                should.exist(err)
                done()
            })
        })
    })

    describe("#failMessage",function(){
         var id;
         beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
                        bqClient.getMessage("testTopic","testConsumer",undefined,function(err,msg){
                            should.not.exist(err)
                            should.exist(msg)
                            id = msg.id
                            done()
                        })
                    })
                })
            })
        })

        it("should move a message to the fails list",function(done){
            bqClient.failMessage("testTopic","testConsumer",id,function(err){
                should.not.exist(err)
                redisClient.execute("lrange","topics:testTopic:consumers:testConsumer:fails",0,-1,function(err,data){
                    should.not.exist(err)
                    data.toString().should.include(id)
                    done()
                })
            }) 
        })
        it("should get an error if consumer group dosn't exist",function(done){
            bqClient.failMessage("testTopic","testConsumer-noExist",id,function(err){
                should.exist(err)
                done()
            })
        })
        it("should return remaining onfailed",function(done){
          bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
              bqClient.getMessage("testTopic","testConsumer",undefined,function(err,msg){
                  should.not.exist(err)
                  should.exist(msg)
                  var id2 = msg.id
                  bqClient.failMessage("testTopic","testConsumer",id,function(err,data){
                    bqClient.failMessage("testTopic","testConsumer",id2,function(err,data){
                      bqClient.getMessage("testTopic","testConsumer",undefined,function(err,msg){
                        msg.remaining.should.equal(1);
                        bqClient.getMessage("testTopic","testConsumer",undefined,function(err,msg){
                          msg.remaining.should.equal(0);
                          done()
                        });
                      });
                    })
                 });
              })
          })
       })
    })

    describe("#listTopics",function(){
        it("should get the topic list",function(done){
            bqClient.listTopics(function(data){
                data.should.be.empty
                bqClient.createTopic("testTopic",function(err){
                    should.not.exist(err)
                    bqClient.listTopics(function(data){
                        data.should.include("testTopic")
                        done()
                    })
                })
            })
        })
    })

    describe("#listConsumerGroups",function(){
        beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                done()
            })
        })

        it("should get the consumer group list for a topic",function(done){
            bqClient.getConsumerGroups("testTopic",function(err,data){
                should.not.exist(err)
                should.exist(data)
                data.should.be.empty
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    should.not.exist(err)
                    bqClient.getConsumerGroups("testTopic",function(err,data){
                        should.not.exist(err)
                        data.should.include("testConsumer")
                        data.should.have.length(1)
                        done()
                    })
                })
            })
        })
        it("should fail if the topic doesn't exist",function(done){
            bqClient.getConsumerGroups("testTopic-noExist",function(err,data){
                should.exist(err)
                done()
            })
        })
    })

    describe("#getNodeStats",function() {
      beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    should.not.exist(err)
                    done();
                });
            });
        });
        it("Should get empty data if no topics", function(done) {
          redisClient.execute("FLUSHALL",function(data,err){
            bqClient.getNodeStats(function(err, data) {
              should.exist(data.sample_date);
              should.exist(data.topics_stats);
              data.topics_stats.length.should.equal(0);
              should.exist(data.node_stats);
              done();
            });  
          });
        });
        it("Should get full data of node", function(done){
          function prepareTest(callback) {
              bqClient.createTopic("testTopic1",function(err){
                bqClient.createConsumerGroup("testTopic","testConsumer2",function(err){
                  bqClient.createConsumerGroup("testTopic1","testConsumer3",function(err){
                    bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,key){
                      callback()
                    });
                  });
                });
              });
            }
            prepareTest(function() {
              bqClient.getNodeStats(function(err, data) {
                should.exist(data.sample_date);
                should.exist(data.topics_stats);
                data.topics_stats.length.should.equal(2);
                done();
              }); 
            });
        });
    });

    describe("#getConsumerGroupStats",function(){
         beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    should.not.exist(err)
                    done()
                })
            })
        })

        it("should get the amount of processing messages",function(done){
            bqClient.getConsumerStats("testTopic","testConsumer",function(err,data){
                should.exist(data)
                should.not.exist(err)
                data.should.have.property("processing")
                data.processing.should.equal(0)
                bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
                    bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
                        bqClient.getConsumerStats("testTopic","testConsumer",function(err,data){
                            should.exist(data)
                            should.not.exist(err)
                            data.should.have.property("processing")
                            data.processing.should.equal(0)
                            bqClient.getMessage("testTopic","testConsumer",undefined,function(err,msg){
                                bqClient.getConsumerStats("testTopic","testConsumer",function(err,data){
                                    should.exist(data)
                                    should.not.exist(err)
                                    data.should.have.property("processing")
                                    data.processing.should.equal(1)
                                    bqClient.getMessage("testTopic","testConsumer",undefined,function(err,msg){
                                        bqClient.getConsumerStats("testTopic","testConsumer",function(err,data){
                                            should.not.exist(err)
                                            should.exist(data)
                                            data.should.have.property("processing")
                                            data.processing.should.equal(2)
                                            done()
                                        })
                                    })
                                })
                            })
                        })
                    })
                })
            })
        })
        it("should get the amount of failed messages",function(done){
            bqClient.getConsumerStats("testTopic","testConsumer",function(err,data){
                should.exist(data)
                should.not.exist(err)
                data.should.have.property("fails")
                data.fails.should.equal(0)
                bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
                    bqClient.getConsumerStats("testTopic","testConsumer",function(err,data){
                        should.exist(data)
                        should.not.exist(err)
                        data.should.have.property("fails")
                        data.fails.should.equal(0)
                        bqClient.getMessage("testTopic","testConsumer",undefined,function(err,msg){
                            bqClient.getConsumerStats("testTopic","testConsumer",function(err,data){
                                should.exist(data)
                                should.not.exist(err)
                                data.should.have.property("fails")
                                data.fails.should.equal(0)
                                bqClient.failMessage("testTopic","testConsumer",msg.id,function(err){
                                    bqClient.getConsumerStats("testTopic","testConsumer",function(err,data){
                                       should.exist(data)
                                       should.not.exist(err)
                                       data.should.have.property("fails")
                                       data.fails.should.equal(1)
                                       done()
                                    })
                                })
                            })
                        })
                    })
                })
            })
        })
        it("should get the amount of unprocess messages",function(done){
            bqClient.getConsumerStats("testTopic","testConsumer",function(err,data){
               should.exist(data)
               should.not.exist(err)
               data.should.have.property("lag")
               data.lag.should.equal(0)
               bqClient.postMessage("testTopic",{msg:"test"},function(err,key){
                   bqClient.getConsumerStats("testTopic","testConsumer",function(err,data){
                       should.exist(data)
                       should.not.exist(err)
                       data.should.have.property("lag")
                       data.lag.should.equal(1)
                       bqClient.getMessage("testTopic","testConsumer",undefined,function(err,msg){
                           bqClient.getConsumerStats("testTopic","testConsumer",function(err,data){
                               should.exist(data)
                               should.not.exist(err)
                               data.should.have.property("lag")
                               data.lag.should.equal(0)
                               done()
                           })
                       })
                   })
               })
            })
        })
        it("should fail if consumer group doesn't exist",function(done){
            bqClient.getConsumerStats("testTopic-doesntExist","testConsumer",function(err,data){
                should.exist(err)
                done()
            })
        })
    })
   
    describe("Heads",function(){
         beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    should.not.exist(err)
                    done()
                })
            })
        })

        it("should response -1 if the head for a topic doesn't exist",function(done){
            bqClient.getHead("testTopic",function(err,data){
                should.not.exist(err)
                data.head.should.equal(-1)
                done()
            })
        })
        it("should return the head of an specific topic",function(done){
            bqClient.postMessage("testTopic",{msg:"test"},function(err,data){
                bqClient.postMessage("testTopic",{msg:"test"},function(err,data){
                    bqClient.getHead("testTopic",function(err,data){
                        should.not.exist(err)
                        data.head.should.equal(""+2)
                        done()
                    })
                })
            })
        })
        it("should return all topic's head",function(done){
            bqClient.createTopic("testTopic2",function(err){
                bqClient.postMessage("testTopic",{msg:"test"},function(err,data){
                    bqClient.postMessage("testTopic",{msg:"test"},function(err,data){
                        bqClient.getHeads(function(err,data){
                            should.not.exist(err)
                            Object.keys(data).length.should.equal(2)
                            data.testTopic.should.equal("2")
                            data.testTopic2.should.equal(-1)
                            done()
                        })
                    })
                })
            })
        })
    })

    describe("Topic data",function(){
        it("should get ttl",function(done){
            bqClient.createTopic("testTopic1",1,function(err){
                should.not.exist(err)
                bqClient.getTopicTtl("testTopic1",function(err,data){
                    should.not.exist(err)
                    should.exist(data)
                    data.should.equal(""+1)
                    bqClient.createTopic("testTopic2",2,function(err){
                        should.not.exist(err)
                        bqClient.getTopicTtl("testTopic2",function(err,data){
                            should.not.exist(err)
                            should.exist(data)
                            data.should.equal(""+2)
                            done()
                        })
                    })
                })
            })
        })
    })

    describe("Reset",function(){
        beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                bqClient.createConsumerGroup("testTopic","testConsumer",function(err){
                    should.not.exist(err)
                    done()
                })
            })
        })

        it("should reset consumers",function(done){
            redisClient.execute("SET","topics:testTopic:head",10,function(err,data){
                should.not.exist(err)
                redisClient.execute("GET","topics:testTopic:consumers:testConsumer:last",function(err,data){
                    should.not.exist(err)
                    data.toString().should.equal("1")
                    bqClient.resetConsumerGroup("testTopic","testConsumer",function(err,data){
                        should.not.exist(err)
                        redisClient.execute("GET","topics:testTopic:consumers:testConsumer:last",function(err,data){
                            should.not.exist(err)
                            data.toString().should.equal("11")
                            done()
                        })
                    })
                })
            })
        })
        it("should fail if consumer doesn't exist",function(done){
            bqClient.resetConsumerGroup("testTopic","testConsumer-unexistent",function(err,data){
                should.exist(err)
                done()
            })
        })
    })

    describe("delete",function(){
        it("should delete topics",function(done){
            bqClient.createTopic("testTopic1",function(err){
                should.not.exist(err)
                bqClient.listTopics(function(data){
                    data.length.should.equal(1)
                    bqClient.deleteTopic("testTopic1",function(err){
                        should.not.exist(err)
                        bqClient.listTopics(function(data){
                            data.length.should.equal(0)
                            done()
                        })
                    })
                })
            })
        })
        it("should delete consumers",function(done){

            bqClient.createTopic("testTopic1",function(err){
                bqClient.createConsumerGroup("testTopic1","testConsumer",function(err){
                    should.not.exist(err)
                    bqClient.getConsumerGroups("testTopic1",function(err,data){
                        should.not.exist(err)
                        data.length.should.equal(1)
                        bqClient.deleteConsumerGroup("testTopic1","testConsumer",function(err,data){
                            should.not.exist(err)
                            bqClient.getConsumerGroups("testTopic1",function(err,data){
                                should.not.exist(err)
                                data.length.should.equal(0)
                                done()
                            })
                        })
                    })
                })
            })
        })
    })
})

