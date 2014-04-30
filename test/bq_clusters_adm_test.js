var should = require('should'),
    ZK = require("zookeeper"),
    redis = require("redis"),
    bq = require('../lib/bq_client.js'),
    bqadm = require("../lib/bq_clusters_adm.js"),
    request = require('request'),
    log = require("node-logging"),
    utils = require('../lib/bq_client_utils.js'),
    bj = require('../lib/bq_journal_client_redis.js'),
    bqc = require('../lib/bq_cluster_client.js'),
    mysql = require("mysql");

describe("Clusters administration for multicluster purposes",function(){
    
    var bqPath = "/bq"
    var clustersPath = bqPath+"/clusters"
   
    var zkConfig = {
        connect: "localhost:2181",
        timeout: 200000,
        debug_level: ZK.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false
    }   

    var mysqlConf = {
        host     : 'localhost',
        user     : 'root',
        password : '',
        database : 'bigqueue'
    };


    var mysqlConn = mysql.createConnection(mysqlConf);

    var admConfig = {
        "zkConfig":zkConfig,
        "zkBqPath":bqPath,
        "createNodeClientFunction":bq.createClient,
        "createJournalClientFunction":bj.createJournalClient,
        "logLevel":"error",
        "defaultCluster":"test1",
        "mysqlConf":mysqlConf
    }
   
    var cluster1Config = {
        "zkConfig":zkConfig,
        "zkClusterPath":clustersPath+"/test1",
        "createJournalClientFunction":bj.createJournalClient,
        "createNodeClientFunction":bq.createClient
    }
    
    var cluster2Config = {
        "zkConfig":zkConfig,
        "zkClusterPath":clustersPath+"/test2",
        "createJournalClientFunction":bj.createJournalClient,
        "createNodeClientFunction":bq.createClient
    }
    
    var admClient 
    var zk = new ZK(zkConfig)
    var redisClient1 
    var redisClient2

    before(function(done) {
      mysqlConn.connect(function(err) {
        done(err);
      });
    });
    before(function(done){
        redisClient1 = redis.createClient(6379,"127.0.0.1")
        redisClient1.on("ready",function(){
            redisClient2= redis.createClient(6380,"127.0.0.1")
            redisClient2.on("ready",function(){
                done()
            })
        })
    }) 

    before(function(done){
        zk.connect(function(err){
            if(err){
                done(err)
            }else{
                done()  
            }
        })
    })

    beforeEach(function(done){
        redisClient1.send_command("flushall", [], function(err){
            redisClient2.send_command("flushall", [], function(){
                done()
            })
        })
    })
    beforeEach(function(done) {
      mysqlConn.query("TRUNCATE stats", function(err) {
        mysqlConn.commit(done);
      });
    });     

    beforeEach(function(done){
        zk.a_create("/bq","",0,function(rc,error,path){
            utils.deleteZkRecursive(zk,"/bq/clusters",function(){
                utils.deleteZkRecursive(zk,"/bq/admin",function(){
                    zk.a_create("/bq/clusters","",0,function(rc,error,path){
                        zk.a_create("/bq/admin","",0,function(rc,error,path){
                            zk.a_create("/bq/admin/indexes","",0,function(rc,error,path){
                                zk.a_create("/bq/admin/indexes/topics","",0,function(rc,error,path){
                                    zk.a_create("/bq/admin/indexes/groups","",0,function(rc,error,path){
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

    beforeEach(function(done){
        admClient = bqadm.createClustersAdminClient(admConfig)
        admClient.on("ready",function(){
            done()
        })
    })

    afterEach(function(done){
        admClient.shutdown()
        process.nextTick(function(){
            done()
        })
    })

    after(function(done){
        zk.close()
        process.nextTick(function(){
            done()
        })
    })

    describe("Create clusters",function(){
        it("should create a cluster",function(done){
            admClient.createBigQueueCluster({name:"test1"},function(err){
               should.not.exist(err)
                zk.a_exists(clustersPath+"/test1",false,function (rc,error,stat){
                    should.exist(rc)
                    rc.should.equal(0)
                    zk.a_exists(clustersPath+"/test1/nodes",false,function (rc,error,stat){
                        should.exist(rc)
                        rc.should.equal(0)
                        zk.a_exists(clustersPath+"/test1/topics",false,function (rc,error,stat){
                            should.exist(rc)
                            rc.should.equal(0)
                            zk.a_exists(clustersPath+"/test1/journals",false,function (rc,error,stat){
                                should.exist(rc)
                                rc.should.equal(0)
                                zk.a_exists(clustersPath+"/test1/endpoints",false,function (rc,error,stat){
                                    should.exist(rc)
                                    rc.should.equal(0)
                                    done()
                                })
                            })
                        })
                    })
                })
            })
        })
        it("should fail if the cluster already exists",function(done){
            admClient.createBigQueueCluster({name:"test1"},function(err){
               should.not.exist(err)
                admClient.createBigQueueCluster({name:"test1"},function(err){
                    should.exist(err)
                    done()
                })
            })

        })

        it("should support add journals on create",function(done){
            var clusterData={
                    name:"test1",
                    journals:[
                        {id:"journal1",host:"journal1","port":123,"status":"DOWN"},
                        {id:"journal2",host:"journal2","port":123,"status":"DOWN"}
                    ]
               }
            admClient.createBigQueueCluster(clusterData,function(err){
                zk.a_get(clustersPath+"/test1/journals/journal1",false,function (rc,error,stat,data){
                    var d = JSON.parse(data)
                    d.host.should.equal("journal1")
                    zk.a_get(clustersPath+"/test1/journals/journal2",false,function (rc,error,stat,data){
                        var d = JSON.parse(data)
                        d.host.should.equal("journal2")
                        done()
                    })
                })
            })
        })

        it("should support add nodes on create",function(done){
            var clusterData={
                    name:"test1",
                    nodes:[
                        {id:"node1",host:"node1","port":123,"status":"DOWN","journals":[]},
                        {id:"node2",host:"node2","port":123,"status":"DOWN","journals":[]}
                    ]
               }
            admClient.createBigQueueCluster(clusterData,function(err){
                zk.a_get(clustersPath+"/test1/nodes/node1",false,function (rc,error,stat,data){
                  JSON.parse(data).host.should.equal("node1")
                    zk.a_get(clustersPath+"/test1/nodes/node2",false,function (rc,error,stat,data){
                        JSON.parse(data).host.should.equal("node2")
                        done()
                    })
                })
            })
        })
        it("should support add entry points on create",function(done){
            var clusterData={
                    name:"test1",
                    endpoints:[
                        {id:"e1",host:"127.0.0.1",port:"8080"},
                        {id:"e2",host:"127.0.0.1",port:"8080"}
                    ]
               }
            admClient.createBigQueueCluster(clusterData,function(err){
                zk.a_get(clustersPath+"/test1/endpoints/e1",false,function (rc,error,stat,data){
                    JSON.parse(data).host.should.equal("127.0.0.1")
                    zk.a_get(clustersPath+"/test1/endpoints/e2",false,function (rc,error,stat,data){
                        JSON.parse(data).host.should.equal("127.0.0.1")
                        done()
                    })
                })
            })
        })
    })

    describe("Modify clusters",function(){
        beforeEach(function(done){
            admClient.createBigQueueCluster({
                name:"test1",
                nodes:[
                    {id:"node1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                    {id:"node2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
                ],
                journals:[
                    {id:"j1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP"}
                ]
            },function(err){
                should.not.exist(err)
                done()
            })
        })
        it("should support add nodes",function(done){
            admClient.addNodeToCluster("test1",{id:"node1","host":"127.0.0.1","port":6381,"errors":0},function(err){
                should.exist(err)
                admClient.addNodeToCluster("test1",{id:"node3"},function(err){
                    should.exist(err)
                    admClient.addNodeToCluster("test1",{id:"node3","host":"127.0.0.1","port":6381,"errors":0,"status":"DOWN","journals":[]},function(err){
                        should.not.exist(err)
                        zk.a_get(clustersPath+"/test1/nodes/node3",false,function(rc,error,stat,data){
                            rc.should.equal(0)
                            var d = JSON.parse(data)
                            d.host.should.equal("127.0.0.1")
                            d.port.should.equal(6381)
                            done()
                        })
                    })
                })
            })
        })

        it("should support servers modify",function(done){
            admClient.updateNodeData("test1",{id:"node3","port":6382},function(err){
                should.exist(err)
                admClient.updateNodeData("test1",{id:"node2","port":6382,"host":"127.0.0.1","status":"DOWN","journals":[]},function(err){
                    should.not.exist(err)
                    zk.a_get(clustersPath+"/test1/nodes/node2",false,function(rc,error,stat,data){
                        rc.should.equal(0)
                        var d = JSON.parse(data)
                        d.host.should.equal("127.0.0.1")
                        d.port.should.equal(6382)
                        admClient.updateNodeData("test1",{id:"node2","port":6383,"host":"1234","description":"test"},function(err){
                            should.not.exist(err)
                            zk.a_get(clustersPath+"/test1/nodes/node2",false,function(rc,error,stat,data){
                                rc.should.equal(0)
                                var d = JSON.parse(data)
                                d.host.should.equal("1234")
                                d.port.should.equal(6383)
                                d.description.should.equal("test")
                                done()
                            })
                        })
                    })
                })
            })
        })

        it("should support add journals",function(done){
            admClient.addJournalToCluster("test1",{id:"j1","host":"127.0.0.1","port":6381,"errors":0},function(err){
                should.exist(err)
                admClient.addJournalToCluster("test1",{id:"j2"},function(err){
                    should.exist(err)
                    admClient.addJournalToCluster("test1",{id:"j2","host":"127.0.0.1","port":6381},function(err){
                        should.not.exist(err)
                        zk.a_get(clustersPath+"/test1/journals/j2",false,function(rc,error,stat,data){
                            rc.should.equal(0)
                            var d = JSON.parse(data)
                            d.host.should.equal("127.0.0.1")
                            d.port.should.equal(6381)
                            done()
                        })
                    })
                })
            })
        })
        it("should support remove journals")
        it("should validate before journal remove that the journal is unused")
    })

    describe("Create topics and groups",function(){
        beforeEach(function(done){
            admClient.createBigQueueCluster({
                    name:"test1",
                    nodes:[
                        {id:"node1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                        {id:"node2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
                    ]
               },function(err){
                   should.not.exist(err)
                   admClient.createBigQueueCluster({
                        name:"test2",
                        nodes:[
                            {id:"node1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                            {id:"node2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
                        ]
                   },function(err){
                       should.not.exist(err)
                       done()
                   })
               })
               admClient.on("ready",function(){
                done()
               })
        })

        it("should enable to create topics into the default clusters",function(done){
            admClient.createTopic("test",undefined,function(err){
                should.not.exist(err)
                var clusterClient = bqc.createClusterClient(cluster1Config)
                clusterClient.on("ready",function(){
                    process.nextTick(function(){
                        clusterClient.listTopics(function(data){
                        should.exist(data)
                        data.should.have.length(1)
                        data[0].should.equal("test")
                        clusterClient.shutdown()
                        done()
                        })
                    })
                })
            })
        })

        it("should enable to create topics with an specific ttl",function(done){
            admClient.createTopic({"name":"test","ttl":100},undefined,function(err){
                should.not.exist(err)
                var clusterClient = bqc.createClusterClient(cluster1Config)
                clusterClient.on("ready",function(){
                    clusterClient.getTopicTtl("test",function(err,data){
                        should.exist(data)
                        data.should.equal(""+100)
                        clusterClient.shutdown()
                        done()
                    })
                })
            })
        })

        it("should enable to create topics into an specific cluster",function(done){
            admClient.createTopic("test","test2",function(err){
                should.not.exist(err)
                var clusterClient = bqc.createClusterClient(cluster2Config)
                clusterClient.on("ready",function(){
                    clusterClient.listTopics(function(data){
                        should.exist(data)
                        data.should.have.length(1)
                        data[0].should.equal("test")
                        done()
                        clusterClient.shutdown()
                    })
                })
            })

        })
       it("should fail if the topic exist in any cluster",function(done){
            admClient.createTopic("test","test1",function(err){
                should.not.exist(err)
                admClient.createTopic("test","test2",function(err){
                    should.exist(err)
                    done()
                })
            })

       })
       it("should enable create consumers in any clusters",function(done){
           admClient.createTopic("test-c1","test1",function(err){
               should.not.exist(err)
               admClient.createTopic("test-c2","test2",function(err){
                   should.not.exist(err)
                   admClient.createConsumerGroup("test-c1","test-consumer-1",function(err){
                       should.not.exist(err)
                       admClient.createConsumerGroup("test-c2","test-consumer-2",function(err){
                           should.not.exist(err)
                           var clusterClient1 = bqc.createClusterClient(cluster1Config)
                           clusterClient1.on("ready",function(){
                               clusterClient1.getConsumerGroups("test-c1",function(err,data){
                                   should.not.exist(err)
                                   should.exist(data)
                                   data.should.have.length(1)
                                   data[0].should.equal("test-consumer-1")
                                   var clusterClient2 = bqc.createClusterClient(cluster2Config)
                                   clusterClient2.on("ready",function(){
                                       clusterClient2.getConsumerGroups("test-c2",function(err,data){
                                           should.not.exist(err)
                                           should.exist(data)
                                           data.should.have.length(1)
                                           data[0].should.equal("test-consumer-2")
                                           done()
                                           clusterClient2.shutdown()
                                           clusterClient1.shutdown()
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

    describe("Delete topics",function(done){
        beforeEach(function(done){
            admClient.createBigQueueCluster({
                    name:"test1",
                    nodes:[
                        {id:"node1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                        {id:"node2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
                    ]
               },function(err){
                   should.not.exist(err)
                   admClient.createBigQueueCluster({
                        name:"test2",
                        nodes:[
                            {id:"node1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                            {id:"node2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
                        ]
                   },function(err){
                       should.not.exist(err)
                       done()
                   })
               })
        })
        beforeEach(function(done){
            admClient.createTopic({"name":"test","group":"test"},"test2",function(err){
                should.not.exist(err)
                var clusterClient = bqc.createClusterClient(cluster2Config)
                clusterClient.on("ready",function(){
                    clusterClient.listTopics(function(data){
                        should.exist(data)
                        data.should.have.length(1)
                        data[0].should.equal("test")
                        done()
                        clusterClient.shutdown()
                    })
                })
            })
        })
        it("should delete topic from specific clusters",function(done){
             admClient.deleteTopic("test",function(err){
                should.not.exist(err)
                var clusterClient = bqc.createClusterClient(cluster2Config)
                clusterClient.on("ready",function(){
                    clusterClient.listTopics(function(data){
                        should.exist(data)
                        data.should.have.length(0)
                        clusterClient.shutdown()
                        done()
                    })
                })
            })
        })
        it("should fail if topic doesn't exist on delete",function(done){
             admClient.deleteTopic("test-no-exist",function(err){
                 should.exist(err)
                 done()
             })
        })
        it("should delete indexes from zookeeper on delete topics",function(done){
            zk.a_exists("/bq/admin/indexes/topics/test",false,function(rc,error,stat){
                rc.should.equal(0)
                zk.a_exists("/bq/admin/indexes/groups/test",false,function(rc,error,stat){
                    rc.should.equal(0)
                    admClient.deleteTopic("test",function(err){
                        zk.a_exists("/bq/admin/indexes/topics/test",false,function(rc,error,stat){
                            rc.should.not.equal(0)
                            zk.a_exists("/bq/admin/indexes/groups/test/test",false,function(rc,error,stat){
                                rc.should.not.equal(0)
                                done()
                            })
                        })
                    })
                })                     
            })
        })
    })

    describe("Delete consumers",function(done){
        beforeEach(function(done){
            admClient.createBigQueueCluster({
                    name:"test1",
                    nodes:[
                        {id:"node1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                        {id:"node2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
                    ]
               },function(err){
                   should.not.exist(err)
                   admClient.createBigQueueCluster({
                        name:"test2",
                        nodes:[
                            {id:"node1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                            {id:"node2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
                        ]
                   },function(err){
                       should.not.exist(err)
                       done()
                   })
               })
        })
        beforeEach(function(done){
            admClient.createTopic({"name":"test","group":"test"},"test2",function(err){
                admClient.createConsumerGroup("test","testConsumer",function(err){
                    should.not.exist(err)
                    done()
                })
            })
        })
        it("should delete consumer from specific cluster",function(done){
            admClient.getTopicData("test",function(err,data){
                should.not.exist(err)
                admClient.deleteConsumerGroup("test","testConsumer",function(err){
                    should.not.exist(err)
                    admClient.getTopicData("test",function(err,data){
                        should.not.exist(err)
                        data.consumers.should.have.length(0)
                        done()
                    })
                })
            })
        })

        it("should fail if consumer doesn't exist on delete",function(done){
            admClient.deleteConsumerGroup("test","testConsumer-no-exist",function(err){
                should.exist(err)
                done()
            })
        })
    })

    describe("Retrieve information",function(){
         beforeEach(function(done){
            admClient.createBigQueueCluster({
                name:"test1",
                nodes:[
                    {id:"node1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                    {id:"node2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
                ],
                journals:[
                    {id:"j1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP"},
                ],
                endpoints:[
                    {id:"e1","host":"127.0.0.1","port":8080},
                    {id:"e2","host":"127.0.0.1","port":8081}
                ]

           },function(err){
               should.not.exist(err)
               admClient.createBigQueueCluster({
                    name:"test2",
                    nodes:[
                        {id:"node1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                        {id:"node2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
                    ],
                    endpoints:[
                        {id:"e1","host":"127.0.0.1","port":8080},
                        {id:"e2","host":"127.0.0.1","port":8081}
                    ]
               },function(err){
                   should.not.exist(err)
                   admClient.createTopic({"name":"test-c1","group":"test"},"test1",function(err){
                       should.not.exist(err)
                       admClient.createTopic({"name":"test-c2","group":"test"},"test2",function(err){
                           should.not.exist(err)
                           done()
                       })
                   })
               })
           })
        })

        it("should return information for an existent node",function(done){
            admClient.getNodeData("test1","node1",function(err,data){
                data.host.should.equal("127.0.0.1")
                data.port.should.equal(6379)
                data.errors.should.equal(0)
                data.status.should.equal("UP")
                should.not.exist(err)
                done()
            })
        })
        it("should return error for an inexistent node",function(done){
            admClient.getNodeData("test1","node5",function(err,data){
                should.exist(err)
                done()
            })
        })
        it("should return information for an existent journal",function(done){
            admClient.getJournalData("test1","j1",function(err,data){
                data.host.should.equal("127.0.0.1")
                data.port.should.equal(6379)
                data.errors.should.equal(0)
                data.status.should.equal("UP")
                should.not.exist(err)
                done()
            })
        })
        it("should return error for an inexistent Journal",function(done){
            admClient.getJournalData("test1","j5",function(err,data){
                should.exist(err)
                done()
            })
        })


        it("should get the entry points for a topic",function(done){
            admClient.getTopicData("test-c1",function(err,data){
                should.not.exist(err)
                should.exist(data)
                data.topic_id.should.equal("test-c1")
                should.exist(data.ttl)
                data.endpoints.should.have.length(2)
                data.endpoints[0].host.should.equal("127.0.0.1")
                data.endpoints[0].port.should.equal(8080)
                data.endpoints[1].host.should.equal("127.0.0.1")
                data.endpoints[1].port.should.equal(8081)
                data.consumers.should.have.length(0)
                admClient.createConsumerGroup("test-c2","test",function(err,data){
                    should.not.exist(err)
                    admClient.getTopicData("test-c2",function(err,data){
                        should.not.exist(err)
                        should.exist(data)
                        data.topic_id.should.equal("test-c2")
                        data.consumers.should.have.length(1)
                        data.endpoints.should.have.length(2)
                        data.endpoints[0].host.should.equal("127.0.0.1")
                        data.endpoints[0].port.should.equal(8080)
                        data.endpoints[1].host.should.equal("127.0.0.1")
                        data.endpoints[1].port.should.equal(8081)
                        done()
                    })
                })
            })
        })
        it("should get data about cluster",function(done){
            admClient.getClusterData("test1",function(err,data){
              data.should.have.keys("cluster","topics","nodes","endpoints","journals")
                done()
            })
        })
        it("should get all topics for a group",function(done){
            admClient.getGroupTopics("test",function(err,data){
                data.should.have.length(2)
                data.should.include("test-c1")
                data.should.include("test-c2")
                done()
            })
        })
    })
 
    describe("Node Stats", function() {
     beforeEach(function(done){
        admClient.createBigQueueCluster({
            name:"test1",
            nodes:[
                {id:"node1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                {id:"node2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
            ],
            journals:[
                {id:"j1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP"},
            ],
            endpoints:[
                {id:"e1","host":"127.0.0.1","port":8080},
                {id:"e2","host":"127.0.0.1","port":8081}
            ]
       },function(err){
        admClient.createTopic({"name":"test-c1","group":"test"},"test1",function(err){
          should.not.exist(err)
          admClient.createConsumerGroup("test-c1","test",function(err,data){
           should.not.exist(err);
           done();
          });
        });
       });
     });


      beforeEach(function(done) {
        mysqlConn.query("TRUNCATE stats", function(err) {
          mysqlConn.commit(done);
        });
      });     
      it("Should receive node stats", function(done) {
        var time = new Date();
        admClient.updateNodeMetrics("test","node1",{
          sample_date: time,
          topic_stats: {
            topic1: {
              consumer1: {
                lag:10,
                fails:2,
                processing:1
              }, 
              consumer2: {
                lag: 1,
                fails: 0,
                processing: 0
              }
            },
            topic2: {
              consumer3: {
                lag:4,
                fails:3,
                processing:1
              }
            }
          }
        }, function(err) {
          should.not.exist(err);
          mysqlConn.query("SELECT * FROM stats", function(err, data) {
            data[0].cluster.should.equal("test");
            data[0].node.should.equal("node1");
            data[1].cluster.should.equal("test");
            data[1].cluster.should.equal("test");
            data[2].node.should.equal("node1");
            data[2].node.should.equal("node1");

            data[0].topic.should.equal("topic1");
            data[0].consumer.should.equal("consumer1");
            data[0].lag.should.equal(10);
            data[0].fails.should.equal(2);
            data[0].processing.should.equal(1);
            
            data[1].topic.should.equal("topic1");
            data[1].consumer.should.equal("consumer2");
            data[1].lag.should.equal(1);
            data[1].fails.should.equal(0);
            data[1].processing.should.equal(0);

            data[2].topic.should.equal("topic2");
            data[2].consumer.should.equal("consumer3");
            data[2].lag.should.equal(4);
            data[2].fails.should.equal(3);
            data[2].processing.should.equal(1);
            
            done();
          });
        });
      });
      it("Should update value", function(done) {
        var time = new Date(); 
        admClient.updateNodeMetrics("test","node1",{
            sample_date: time,
            topic_stats: {
              topic1: {
                consumer1: {
                  lag:10,
                  fails:2,
                  processing:1
                }, 
              }
            }
          }, function(err) {
          admClient.updateNodeMetrics("test","node1",{
              sample_date: time,
              topic_stats: {
                topic1: {
                  consumer1: {
                    lag:1,
                    fails:9,
                    processing:3
                  }, 
                }
              }
            }, function(err) {
              should.not.exist(err);
              mysqlConn.query("SELECT * FROM stats", function(err, data) {

                data[0].cluster.should.equal("test");
                data[0].node.should.equal("node1");
                data[0].topic.should.equal("topic1");
                data[0].consumer.should.equal("consumer1");
                data[0].lag.should.equal(1);
                data[0].fails.should.equal(9);
                data[0].processing.should.equal(3);
                done();
              });
            });
          });
      });
      it("Should fails if some data is missing", function(done) {
        admClient.updateNodeMetrics("test","node1",{
          sample_date: new Date().getTime(),
          topic_stats: {
            topic1: {
              consumer1: {
                lag:10,
                fails:2,
                processing:1
              }, 
              consumer2: {
                lag: 1,
                processing: 0
              }
            }
          }
        }, function(err) {
          should.exist(err); 
          done()
      });
    });
    it("Should get consolidate data for topic", function(done) {
      admClient.updateNodeMetrics("test1","node1",{
          sample_date: new Date().getTime(),
          topic_stats: {
            "test-c1": {
              test: {
                lag:10,
                fails:2,
                processing:1
              }, 
            }
          }
        }, function(err) {
          admClient.updateNodeMetrics("test1","node2",{
            sample_date: new Date().getTime(),
            topic_stats: {
              "test-c1": {
                test: {
                  lag:2,
                  fails:3,
                  processing:4
                }, 
              }
            }
          }, function(err) {
            should.not.exist(err);
            admClient.getTopicData("test-c1", function(err, data) {
              should.not.exist(err);
              should.exist(data);
              data.consumers.length.should.equal(1);
              data.consumers[0].consumer_id.should.equal("test");
              data.consumers[0].stats.lag.should.equal(12);
              data.consumers[0].stats.fails.should.equal(5);
              data.consumers[0].stats.processing.should.equal(5);
              done();
            }); 
          });
       });
    });
    it("Should get consolidate date for consummer", function(done) {
      admClient.updateNodeMetrics("test1","node1",{
          sample_date: new Date().getTime(),
          topic_stats: {
            "test-c1": {
              test: {
                lag:10,
                fails:2,
                processing:1
              }, 
            }
          }
        }, function(err) {
          admClient.updateNodeMetrics("test1","node2",{
            sample_date: new Date().getTime(),
            topic_stats: {
              "test-c1": {
                test: {
                  lag:2,
                  fails:3,
                  processing:4
                }, 
              }
            }
          }, function(err) {
            should.not.exist(err);
            admClient.getConsumerData("test-c1", "test", function(err, data) {
              should.not.exist(err);
              should.exist(data);
              data.consumer_stats.lag.should.equal(12);
              data.consumer_stats.fails.should.equal(5);
              data.consumer_stats.processing.should.equal(5);
              done();
            }); 
          });
       });

    });
  });

});

