var should = require("should"),
    ZK = require("zookeeper"),
    oc = require("../lib/bq_cluster_orchestrator.js"),
    bq = require("../lib/bq_client.js"),
    bj = require("../lib/bq_journal_client_redis.js"),
    redis = require("redis"),
    log = require("node-logging"),
    utils = require("../lib/bq_client_utils.js"),
    os = require("os");

describe("Orchestrator",function(){
    
    var redisClient1

    var redisClient2

    var zkConfig = {
            connect: "localhost:2181",
            timeout: 200000,
            debug_level: ZK.ZOO_LOG_LEVEL_WARN,
            host_order_deterministic: false
        }   

    var ocConfig = {
        "zkClustersPath":"/bq/clusters/test",
        "zkConfig":zkConfig,
        "createNodeClientFunction":bq.createClient,
        "createJournalClientFunction":bj.createJournalClient,
        "checkInterval":50,
        "logLevel":"critical",
        "amountOfOrchestratorToSetDownANode":1

    }
    var zk = new ZK(zkConfig)

    before(function(done){
         log.setLevel("critical")
         redisClient1 = redis.createClient()
         redisClient2 = redis.createClient(6380,"127.0.0.1")
         zk.connect(function(err){
            if(err){
                done(err)
            }else{
                done()  
            }
        })
    });

    after(function(done){
        zk.close()
        process.nextTick(function(){
            done()
        })
    })

    beforeEach(function(done){
        utils.deleteZkRecursive(zk,"/bq",function(){
            zk.a_create("/bq","",0,function(rc,error,path){    
                zk.a_create("/bq/clusters","",0,function(rc,error,path){
                    zk.a_create("/bq/clusters/test","",0,function(rc,error,path){
                       zk.a_create("/bq/clusters/test/topics","",0,function(rc,error,path){
                           zk.a_create("/bq/clusters/test/journals","",0,function(rc,error,path){
                                zk.a_create("/bq/clusters/test/nodes","",0,function(rc,error,path){
                                    zk.a_create("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"UP"}),0,function(rc,error,path){
                                        zk.a_create("/bq/clusters/test/nodes/redis2",JSON.stringify({"host":"127.0.0.1","port":6380,"errors":0,"status":"UP"}),0,function(rc,error,path){
                                            zk.a_create("/bq/clusters/test/journals/j1",
                                                    JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"UP","start_date":new Date()}),0,function(rc,error,path){
                                                zk.a_create("/bq/clusters/test/journals/j2",
                                                        JSON.stringify({"host":"127.0.0.1","port":6380,"errors":0,"status":"UP","start_date":new Date()}),0,function(rc,error,path){
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
            })
        })
    }) 

    beforeEach(function(done){
        redisClient1.flushall(function(err,data){
            redisClient2.flushall(function(err,data){
                done()
            })
        })
    })

    /*
     * From version 0.2.0 orchestrator only check nodes periodically
     */
    /*it("Should check nodes status when any event is produced",function(done){
        var orch = oc.createOrchestrator(ocConfig)
        orch.on("ready",function(){
        setTimeout(function(){
            zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":1,"status":"DOWN"}),-1,function(){
                setTimeout(function(){
                    zk.a_get("/bq/clusters/test/nodes/redis1",false,function(rc,error,stat,data){
                       should.exist(rc)
                       rc.should.equal(0)
                       should.exist(data)
                       var d = JSON.parse(data)
                       d.status.should.equal("UP")
                       d.errors.should.equal(0)
                       done()
                       orch.shutdown()
                    })
                },100)
            })
        },200)
        })
    })*/
    /*
     * From version 0.2.0 orchestrator only check nodes periodically
     */
   /* it("Should check nodes periodically looking for connection problems",function(done){
        var orch = oc.createOrchestrator(ocConfig)
        orch.on("ready",function(){
            setTimeout(function(){
                zk.a_create("/bq/clusters/test/nodes/redis3",JSON.stringify({"host":"127.0.0.1","port":6381,"errors":0,"status":"UP"}),0,function(rc,error,path){
                    setTimeout(function(){
                        zk.a_get("/bq/clusters/test/nodes/redis3",false,function(rc,error,stat,data){
                           should.exist(rc)
                           rc.should.equal(0)
                           should.exist(data)
                           var d = JSON.parse(data)
                           d.status.should.equal("DOWN")
                           done()
                           orch.shutdown()
                        })
                    },1000)
                })
            },200)
        })
    })*/
    it("Should refresh nodes on nodeMonitor",function(done){
        var orch = oc.createOrchestrator(ocConfig)
        orch.on("ready",function(){
            setTimeout(function(){
                zk.a_create("/bq/clusters/test/nodes/redis3",JSON.stringify({"host":"127.0.0.1","port":6381,"errors":0,"status":"UP"}),0,function(rc,error,path){
                    setTimeout(function(){
                        zk.a_get("/bq/clusters/test/nodes/redis3",false,function(rc,error,stat,data){
                           should.exist(rc)
                           rc.should.equal(0)
                           should.exist(data)
                           var d = JSON.parse(data)
                           d.status.should.equal("DOWN")
                           done()
                           orch.shutdown()
                        })
                    },300)
                })
            },300)
        })
    })
    it("Should check nodes periodically looking for inconsistencies and sync if one is found",function(done){
        zk.a_create("/bq/clusters/test/topics/test2","",0,function(rc,error,path){
            zk.a_create("/bq/clusters/test/topics/test2/consumerGroups","",0,function(rc,error,path){
                zk.a_create("/bq/clusters/test/topics/test2/consumerGroups/testConsumer","",0,function(rc,error,path){
                    redisClient1.exists("topics:test2:ttl",function(err,data){
                        data.should.equal(0)
                        redisClient1.sismember("topics","test2",function(err,data){
                            data.should.equal(0)
                            redisClient1.exists("topics:test2:consumers:testConsumer:last",function(err,data){
                                data.should.equal(0)
                                redisClient2.exists("topics:test2:ttl",function(err,data){
                                    data.should.equal(0)
                                    redisClient2.exists("topics:test2:consumers:testConsumer:last",function(err,data){
                                        data.should.equal(0)
                                        redisClient2.sismember("topics","test2",function(err,data){
                                        data.should.equal(0)
                                            var orch = oc.createOrchestrator(ocConfig)
                                            orch.on("ready",function(){
                                                setTimeout(function(){
                                                    redisClient1.exists("topics:test2:ttl",function(err,data){
                                                        data.should.equal(1)
                                                        redisClient1.exists("topics:test2:consumers:testConsumer:last",function(err,data){
                                                            data.should.equal(1)
                                                            redisClient1.sismember("topics","test2",function(err,data){
                                                                data.should.equal(1)
                                                                redisClient2.sismember("topics","test2",function(err,data){
                                                                    data.should.equal(1)
                                                                    redisClient2.exists("topics:test2:ttl",function(err,data){
                                                                        redisClient2.exists("topics:test2:consumers:testConsumer:last",function(err,data){
                                                                            redisClient2.sismember("topics","test2",function(err,data){
                                                                                 data.should.equal(1)
                                                                                 done()
                                                                                 orch.shutdown()
                                                                            })
                                                                        })
                                                                    })
                                                                })
                                                            })
                                                        })
                                                    })
                                                },500)
                                            })
                                        })
                                    })
                                })
                            })
                        })
                    })
                })
            })
        })
    })
    it("Should sync new nodes",function(done){
        zk.a_create("/bq/clusters/test/topics/test2","",0,function(rc,error,path){
            zk.a_create("/bq/clusters/test/topics/test2/consumerGroups","",0,function(rc,error,path){
                zk.a_create("/bq/clusters/test/topics/test2/consumerGroups/testConsumer","",0,function(rc,error,path){
                    zk.a_delete_("/bq/clusters/test/nodes/redis1",-1,function(rc,err){
                        var orch = oc.createOrchestrator(ocConfig)
                        orch.on("ready",function(){
                            redisClient1.sismember("topics","test2",function(err,data){
                                data.should.equal(0)
                                redisClient1.exists("topics:test2:consumers:testConsumer:last",function(err,data){
                                    data.should.equal(0)
                                    redisClient1.sismember("topics","test2",function(err,data){
                                        data.should.equal(0)
                                        zk.a_create("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"DOWN"}),0,function(rc,error,path){
                                            setTimeout(function(){
                                                redisClient1.sismember("topics","test2",function(err,data){
                                                    data.should.equal(1)
                                                    redisClient1.exists("topics:test2:consumers:testConsumer:last",function(err,data){
                                                        data.should.equal(1)
                                                         redisClient1.sismember("topics","test2",function(err,data){
                                                            data.should.equal(1)
                                                            zk.a_get("/bq/clusters/test/nodes/redis1",false,function(rc,error,stat,data){
                                                               var d = JSON.parse(data)
                                                               d.status.should.equal("UP")
                                                               done()
                                                               orch.shutdown()
                                                            })
                                                         })
                                                    })
                                                })
                                            },500);
                                        }) 
                                    })
                                })
                            })
                        })
                    })
                })
            })
        })

    })
    it("Should not sync nodes in FORCEDOWN status",function(done){
        var orch = oc.createOrchestrator(ocConfig)
        orch.on("ready",function(){
            setTimeout(function(){
                zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":1,"status":"FORCEDOWN"}),-1,function(){
                    setTimeout(function(){
                        zk.a_get("/bq/clusters/test/nodes/redis1",false,function(rc,error,stat,data){
                           should.exist(rc)
                           rc.should.equal(0)
                           should.exist(data)
                           var d = JSON.parse(data)
                           d.status.should.equal("FORCEDOWN")
                           done()
                           orch.shutdown()
                        })
                    },100)
                })
            },200)
        })
    })
    
    it("should check the journals status when any event is produced",function(done){
       var orch = oc.createOrchestrator(ocConfig)
       orch.on("ready",function(){
            zk.a_set("/bq/clusters/test/journals/j1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":1,"status":"DOWN","start_date": new Date()}),-1,function(){
                setTimeout(function(){
                    zk.a_get("/bq/clusters/test/journals/j1",false,function(rc,error,stat,data){
                        should.exist(data)
                        var d = JSON.parse(data)
                        d.status.should.equal("UP")
                        d.errors.should.equal(0)
                        done()
                        orch.shutdown()
                    })
                },300)
            })
       })
    })
    it("should put down a journal when it's the node is down",function(done){
       var orch = oc.createOrchestrator(ocConfig)
       orch.on("ready",function(){
            zk.a_create("/bq/clusters/test/journals/j3",JSON.stringify({"host":"127.0.0.1","port":6381,"errors":0,"status":"UP","start_date":new Date()}),0,function(rc,error,path){
                setTimeout(function(){
                    zk.a_get("/bq/clusters/test/journals/j3",false,function(rc,error,stat,data){
                        should.exist(data)
                        var d = JSON.parse(data)
                        d.status.should.equal("DOWN")
                        d.errors.should.equal(0)
                        done()
                        orch.shutdown()
                    })
                },300)
            }) 
        })
    })
    
    it("should check journals periodicly for connection problems",function(done){
       zk.a_create("/bq/clusters/test/journals/j3",JSON.stringify({"host":"127.0.0.1","port":6381,"errors":0,"status":"UP","start_date":new Date()}),0,function(rc,error,path){
           var orch = oc.createOrchestrator(ocConfig)
           orch.on("ready",function(){
                setTimeout(function(){
                    zk.a_get("/bq/clusters/test/journals/j3",false,function(rc,error,stat,data){
                        should.exist(data)
                        var d = JSON.parse(data)
                        d.status.should.equal("DOWN")
                        d.errors.should.equal(0)
                        done()
                        orch.shutdown()
                    })
                },300)
           })
        })
    })

    it("should put down all nodes related when a journal goes down",function(done){
        var orch = oc.createOrchestrator(ocConfig)
        orch.on("ready",function(){
            zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":["j3"]}),-1,function(){
                zk.a_create("/bq/clusters/test/journals/j3",JSON.stringify({"host":"127.0.0.1","port":6381,"errors":0,"status":"UP","start_date":new Date()}),0,function(rc,error,path){
                    setTimeout(function(){
                         zk.a_get("/bq/clusters/test/nodes/redis1",false,function(rc,error,stat,data){
                            should.exist(data)
                            var d = JSON.parse(data)
                            d.status.should.equal("DOWN")
                            d.errors.should.equal(0)
                            done()
                            orch.shutdown()
                        })
                    },300)
                })
            })
        })
    })
    
    /*
     * From 0.2.0 Nodes detect if their journals are down removing the double check
     */
    /*it("should put a node down if same dependent journal is down",function(done){
        var orch = oc.createOrchestrator(ocConfig)
        orch.on("ready",function(){
            zk.a_create("/bq/clusters/test/journals/j3",JSON.stringify({"host":"127.0.0.1","port":6381,"errors":0,"status":"UP","start_date":new Date()}),0,function(rc,error,path){
                zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":["j3"]}),-1,function(){
                    setTimeout(function(){
                         zk.a_get("/bq/clusters/test/nodes/redis1",false,function(rc,error,stat,data){
                            should.exist(data)
                            var d = JSON.parse(data)
                            d.status.should.equal("DOWN")
                            d.errors.should.equal(0)
                            done()
                            orch.shutdown()
                        })
                    },500)

                })
            })

        })
    })*/

    it("should re-sink a down datanode that goes up",function(done){
         zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":["j2"]}),-1,function(){
             var orch = oc.createOrchestrator(ocConfig)
             orch.on("ready",function(){
                 zk.a_create("/bq/clusters/test/topics/test2","",0,function(rc,error,path){
                    zk.a_create("/bq/clusters/test/topics/test2/consumerGroups","",0,function(rc,error,path){
                        zk.a_create("/bq/clusters/test/topics/test2/consumerGroups/testConsumer","",0,function(rc,error,path){
                            //Wait for sync
                            setTimeout(function(){
                                redisClient1.get("topics:test2:head",function(err,data){
                                    should.not.exist(data)
                                    var journal = bj.createJournalClient({"host":"127.0.0.1","port":6380,"errors":0,"status":"UP","start_date":new Date()})
                                    journal.on("ready",function(){
                                        journal.write("redis1","test2", 1, {"msg":"test"}, 120,function(){
                                            journal.write("redis1","test2", 2, {"msg":"test"}, 120,function(){
                                                zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"DOWN","journals":["j2"]}),-1,function(){
                                                     setTimeout(function(){
                                                        redisClient1.get("topics:test2:head",function(err,data){
                                                            data.should.equal(""+2)
                                                            redisClient1.hgetall("topics:test2:messages:1",function(err,data){
                                                                should.not.exist(err)
                                                                should.exist(data)
                                                                data.msg.should.equal("test") 
                                                                setTimeout(function(){
                                                                     zk.a_get("/bq/clusters/test/nodes/redis1",false,function(rc,error,stat,data){
                                                                        should.exist(data)
                                                                        var d = JSON.parse(data)
                                                                        d.status.should.equal("UP")
                                                                        d.errors.should.equal(0)
                                                                        orch.shutdown()
                                                                        done()
                                                                    })
                                                                },300)
                                                            })
                                                        })
                                                     },300)
                                                 })
                                            })
                                        })
                                    })
                                })
                            },300)
                        })
                    })
                })
             })
        })
       
    })
    it("should use the ttl specified into the topic on re-sink",function(done){
         zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":["j2"]}),-1,function(){
             var orch = oc.createOrchestrator(ocConfig)
             orch.on("ready",function(){
                 zk.a_create("/bq/clusters/test/topics/test2",JSON.stringify({"ttl":10}),0,function(rc,error,path){
                    zk.a_create("/bq/clusters/test/topics/test2/consumerGroups","",0,function(rc,error,path){
                        zk.a_create("/bq/clusters/test/topics/test2/consumerGroups/testConsumer","",0,function(rc,error,path){
                            //Wait for sync
                            setTimeout(function(){
                                redisClient1.get("topics:test2:head",function(err,data){
                                    should.not.exist(data)
                                    zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"DOWN","journals":["j2"]}),-1,function(){
                                        setTimeout(function(){
                                            redisClient1.get("topics:test2:ttl",function(err,data){
                                                data.should.equal(""+10)
                                                orch.shutdown()
                                                done()
                                            })
                                        },300)
                                    })
                                })
                            },300)
                        })
                    })
                })
             })
        })
    })

    it("should reset the up date of a journal when it goes up",function(done){
        var orch = oc.createOrchestrator(ocConfig)
        orch.on("ready",function(){
            var date = new Date()
            zk.a_set("/bq/clusters/test/journals/j2",JSON.stringify({"host":"127.0.0.1","port":6379,"errors":0,"status":"DOWN","start_date":date}),-1,function(){
                setTimeout(function(){
                    zk.a_get("/bq/clusters/test/journals/j2",false,function(rc,error,stat,data){
                        var d = JSON.parse(data)
                        var newDate = new Date(d.start_date)
                        newDate.getMilliseconds().should.not.equal(date.getMilliseconds())
                        done()
                        orch.shutdown()
                    })
                },300)
            })
        })
    })

    it("should not sync removed topics",function(done){
        zk.a_create("/bq/clusters/test/topics/test2","",0,function(rc,error,path){
            zk.a_create("/bq/clusters/test/topics/test2/consumerGroups","",0,function(rc,error,path){
               var orch = oc.createOrchestrator(ocConfig)
               orch.on("ready",function(){
                    setTimeout(function(){
                        redisClient1.sismember("topics","test2",function(err,data){
                            data.should.equal(1)
                            utils.deleteZkRecursive(zk,"/bq/clusters/test/topics/test2",function(rc,err){
                                redisClient1.srem("topics","test2",function(err,data){
                                    setTimeout(function(){
                                        redisClient1.sismember("topics","test2",function(err,data){
                                            data.should.equal(0)
                                            orch.shutdown()
                                            done()
                                        })
                                    },300)
                                })
                            })

                        })

                    },300)
               })
            })
        })
    })
    it("should not sync removed groups",function(done){
        zk.a_create("/bq/clusters/test/topics/test2","",0,function(rc,error,path){
            zk.a_create("/bq/clusters/test/topics/test2/consumerGroups","",0,function(rc,error,path){
                zk.a_create("/bq/clusters/test/topics/test2/consumerGroups/test","",0,function(rc,error,path){
                    zk.a_create("/bq/clusters/test/topics/test2/consumerGroups/test1","",0,function(rc,error,path){
                        var orch = oc.createOrchestrator(ocConfig)
                        orch.on("ready",function(){
                            setTimeout(function(){
                                redisClient1.sismember("topics:test2:consumers","test",function(err,data){
                                    data.should.equal(1)
                                    redisClient1.sismember("topics:test2:consumers","test1",function(err,data){
                                        data.should.equal(1)
                                        utils.deleteZkRecursive(zk,"/bq/clusters/test/topics/test2/consumerGroups/test",function(rc,err){
                                            redisClient1.srem("topics:test2:consumers","test",function(err,data){
                                                setTimeout(function(){
                                                    redisClient1.sismember("topics:test2:consumers","test1",function(err,data){
                                                        data.should.equal(1)
                                                        redisClient1.sismember("topics:test2:consumers","test",function(err,data){
                                                            data.should.equal(0)
                                                            orch.shutdown()
                                                            done()
                                                        })
                                                    })
                                                },300)
                                            })
                                        })
                                    })
                                })
                            },300)
                        })
                    })
                })
            })
        })
    })
    it("Should set DOWN node only if the amount of orchestrator seeing the error is more than or equals the configured",function(done){
        var ocConfig = {
            "zkClustersPath":"/bq/clusters/test",
            "zkConfig":zkConfig,
            "createNodeClientFunction":bq.createClient,
            "createJournalClientFunction":bj.createJournalClient,
            "checkInterval":50,
            "logLevel":"critical",
            "amountOfOrchestratorToSetDownANode":3
        }

        var orch = oc.createOrchestrator(ocConfig)
        orch.on("ready",function(){
            zk.a_create("/bq/clusters/test/nodes/redis3",JSON.stringify({"host":"127.0.0.1","port":6381,"errors":0,"status":"UP"}),0,function(rc,error,path){
                setTimeout(function(){
                    zk.a_get("/bq/clusters/test/nodes/redis3",false,function(rc,error,stat,data){
                       should.exist(rc)
                       rc.should.equal(0)
                       should.exist(data)
                       var d = JSON.parse(data)
                       d.status.should.equal("UP")
                       d.orchestrator_errors.length.should.equal(1)
                       d.orchestrator_errors.push("orch1")
                       d.orchestrator_errors.push("orch2")
                       zk.a_set("/bq/clusters/test/nodes/redis3",JSON.stringify(d),-1,function(rc,error,path){
                            rc.should.equal(0)
                        setTimeout(function(){
                              zk.a_get("/bq/clusters/test/nodes/redis3",false,function(rc,error,stat,data){
                                   should.exist(rc)
                                   rc.should.equal(0)
                                   should.exist(data)
                                   var d = JSON.parse(data)
                                   d.status.should.equal("DOWN")
                                   d.orchestrator_errors.length.should.equal(3)
                                   orch.shutdown() 
                                   done()
                               })
                           },300)
                        })
                    },300)
                },300)
            })
        })
    })
    it("Should set a node UP only if the amount of orchestrator seeing the error is less than the configured",function(done){
        var ocConfig = {
            "zkClustersPath":"/bq/clusters/test",
            "zkConfig":zkConfig,
            "createNodeClientFunction":bq.createClient,
            "createJournalClientFunction":bj.createJournalClient,
            "checkInterval":50,
            "logLevel":"critical",
            "amountOfOrchestratorToSetDownANode":3
        }
        var nodeData = {"host":"127.0.0.1","port":6379,"errors":0,"status":"UP"}
        zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify(nodeData),-1,function(){
             var orch = oc.createOrchestrator(ocConfig)
             orch.on("ready",function(){
                nodeData.status = "DOWN"
                nodeData.orchestrator_errors=["orch1","orch2",os.hostname()]
                zk.a_set("/bq/clusters/test/nodes/redis1",JSON.stringify(nodeData),-1,function(){
                    setTimeout(function(){
                          zk.a_get("/bq/clusters/test/nodes/redis1",false,function(rc,error,stat,data){
                               should.exist(rc)
                               rc.should.equal(0)
                               should.exist(data)
                               var d = JSON.parse(data)
                               d.status.should.equal("UP")
                               d.orchestrator_errors.length.should.equal(2)
                               orch.shutdown() 
                               done()
                          })
                    },300)
                })
             })
        })

    })
})
