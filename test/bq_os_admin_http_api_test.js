var should = require('should'),
    ZK = require("zookeeper"),
    http = require('http'),
    redis = require("redis"),
    bq = require('../lib/bq_client.js'),
    httpApi = require("../ext/openstack/bq_os_admin_http_api.js")
    request = require('request'),
    log = require("node-logging"),
    utils = require('../lib/bq_client_utils.js'),
    bj = require('../lib/bq_journal_client_redis.js'),
    express = require('express')

describe("openstack admin http api",function(){
   
    var bqPath = "/bq"
    
    var zkConfig = {
        connect: "localhost:2181",
        timeout: 200000,
        debug_level: ZK.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false
    }   

    var admConfig = {
        "zkConfig":zkConfig,
        "zkBqPath":bqPath,
        "createNodeClientFunction":bq.createClient,
        "createJournalClientFunction":bj.createJournalClient,
        "logLevel":"error",
        "defaultCluster":"test"
    }
    
    var httpConfig = {
        "admConfig":admConfig,
        "port":8080,
        "maxTtl":500,
        "basePath":"/bigqueue",
        "groupEntity":"group",
        "logLevel":"critical"
    }

    var keystoneConfig = {
        "keystoneUrl":"http://localhost:35357/v2.0",
        "adminToken":"admin",
        "foceAuth":false
    }

    var zk = new ZK(zkConfig)

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
    var redisCli =  redis.createClient()
        redisCli.on("ready",function(){
            redisCli.flushall(function(err){
                should.not.exist(err)
                redisCli.quit()
                done()
            })
        })
    })

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

    after(function(){
        zk.close()
    })

    var api

    beforeEach(function(){
        api = httpApi.startup(httpConfig)
    })
    afterEach(function(){
        api.shutdown()
    })


    describe("Cluster Admin",function(){
        it("should enable to create new clusters",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/clusters",
                method:"GET",
                json:true
            },function(error,response,body){
                should.not.exist(error)
                response.statusCode.should.equal(200)
                body.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/clusters",
                    method:"POST",
                    json:{"name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/clusters",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        response.statusCode.should.equal(200)
                        body.should.have.length(1)
                        body[0].should.equal("test")
                        done()
                    })
                })
            })
        })
        it("Should get the cluster information",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.cluster.should.equal("test")
                    done()
                })
            })
        })
        it("should support cluster deletes")

    })
    
    describe("Topics and consumers",function(){
        beforeEach(function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/clusters",
                method:"POST",
                json:{"name":"test",
                      "nodes":[{
                        "name":"redis1", 
                        "config":{
                            "host":"127.0.0.1",
                            "port":6379,
                            "status":"UP"
                         }
                      }]}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/clusters",
                    method:"POST",
                    json:{"name":"test2",
                          "nodes":[{
                            "name":"redis1", 
                            "config":{ 
                                "host":"127.0.0.1",
                                "port":6379,
                                "status":"UP"
                            }
                         }]}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    done()
                })
            })
        })
        it("should support create topics into the default cluster using your tenant id",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/clusters/test",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(200)
                body.topics.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics",
                    method:"POST",
                    json:{"group":"1234","name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/clusters/test",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        response.statusCode.should.equal(200)
                        body.topics.should.have.length(1)
                        done()
                    })
                })
            })
        })
        it("should support create topics into any cluster using your tenant id",function(done){
             request({
                url:"http://127.0.0.1:8080/bigqueue/clusters/test2",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(200)
                body.topics.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics",
                    method:"POST",
                    json:{"group":"1234","name":"test","cluster":"test2"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/clusters/test2",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        response.statusCode.should.equal(200)
                        body.topics.should.have.length(1)
                        done()
                    })
                })
            })
        })
        it("should support create consumers in any topic using your tenant id",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/topics",
                method:"POST",
                json:{"group":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics/1234-test",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.consumers.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/topics/1234-test/consumers",
                        method:"POST",
                        json:{"group":"456","name":"test-consumer"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8080/bigqueue/topics/1234-test",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.consumers.should.have.length(1)
                            body.consumers[0].consumer.should.equal("456-test-consumer")
                            done()
                        })
                    })
                })
            })
        })
        it("should support list all topics of a group",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/topics",
                method:"POST",
                json:{"group":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics?group=1234",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.length(1)
                    body.should.include("1234-test")
                     request({
                        url:"http://127.0.0.1:8080/bigqueue/topics",
                        method:"POST",
                        json:{"group":"1234","name":"test1"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8080/bigqueue/topics?group=1234",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.should.have.length(2)
                            body.should.include("1234-test")
                            body.should.include("1234-test1")
                            done()
                        })
                    })
                })
            })

        })
        
        it("should get all topic data on create",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/topics",
                method:"POST",
                json:{"group":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                body.should.have.property("topic_id","1234-test")
                body.should.have.property("ttl")
                body.should.have.property("cluster")
                body.should.have.property("entrypoints")
                body.should.have.property("consumers")
                done()
            })

        })
        it("should get all consumer data on create",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/topics",
                method:"POST",
                json:{"group":"1234","name":"test"}
            },function(error,response,body){
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics/1234-test/consumers",
                    method:"POST",
                    json:{"group":"456","name":"test1"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    body.should.have.property("topic_id","1234-test")
                    body.should.have.property("consumer_id","456-test1")
                    body.should.have.property("ttl")
                    body.should.have.property("cluster")
                    body.should.have.property("entrypoints")
                    body.should.have.property("consumer_stats")
                    done()
                })
            })

        })
        
        it("should get all information about a topic",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/topics",
                method:"POST",
                json:{"group":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics/1234-test",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.property("topic_id","1234-test")
                    body.should.have.property("ttl")
                    body.should.have.property("cluster")
                    body.should.have.property("entrypoints")
                    body.should.have.property("consumers")
                    done()
                })
            })

        })
        it("should get all information about a consumer",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/topics",
                method:"POST",
                json:{"group":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics/1234-test/consumers",
                    method:"POST",
                    json:{"group":"456","name":"test1"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/topics/1234-test/consumers/456-test1",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        response.statusCode.should.equal(200)
                        body.should.have.property("topic_id","1234-test")
                        body.should.have.property("consumer_id","456-test1")
                        body.should.have.property("ttl")
                        body.should.have.property("cluster")
                        body.should.have.property("entrypoints")
                        body.should.have.property("consumer_stats")
                        done()
                    })
                })
            })
        })
        
        it("should support list all consumer of a topic",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/topics",
                method:"POST",
                json:{"group":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics/1234-test/consumers",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/topics/1234-test/consumers",
                        method:"POST",
                        json:{"group":"456","name":"test"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8080/bigqueue/topics/1234-test/consumers",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.should.have.length(1)
                            body[0].consumer.should.equal("456-test")
                            done()
                        })
                    })
                })
            })

        })

        it("should support topic deletes")
        it("should support consumers delete")
    })

    describe("Limits",function(done){
        it("should limit the ttl time to the default ttl",function(done){
            request({
                    url:"http://127.0.0.1:8080/bigqueue/clusters",
                    method:"POST",
                    json:{"name":"test",
                    "nodes":[{
                        "name":"redis1", 
                        "config":{
                            "host":"127.0.0.1",
                            "port":6379,
                            "status":"UP"
                         }
                    }]}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics",
                    method:"POST",
                    json:{"group":"1234","name":"test","ttl":500}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/topics",
                        method:"POST",
                        json:{"group":"1234","name":"test","ttl":501}
                    },function(error,response,body){
                        response.statusCode.should.equal(406)
                        done()
                    })
                })
            })
        })
    })

    describe("Keystone authorization", function(){
        var fakekeystone
        before(function(){
            fakekeystone = express.createServer()
            fakekeystone.get("/v2.0/tokens/:token",function(req,res){
                if(req.params.token === "user123" && req.headers["x-auth-token"] === "admin"){
                    return res.json({
                        "access": {
                            "token":{
                                "tenants":[
                                     {
                                         "id": "1", 
                                         "name": "1234"
                                     }
                                ]
                            }
                        }

                    },200)
                }
                if(req.params.token === "someone" && req.headers["x-auth-token"] === "admin"){
                    return res.json({
                        "access": {
                            "token":{
                                "tenants":[
                                     {
                                         "id": "2", 
                                         "name": "someone"
                                     }
                                ]
                            }
                        }

                    },200)
                }

                return res.json({err:"token not found"},404)
            })
            fakekeystone.listen(35357)
        })

        after(function(){
            if(fakekeystone){
                fakekeystone.close()
            }
        })
        
        beforeEach(function(){
            api.shutdown()
            var intConf = httpConfig
            intConf["keystoneConfig"] = keystoneConfig 
            api = httpApi.startup(intConf)
        })

        it("Should validate the token at cluster creation",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(401)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/clusters",
                    method:"POST",
                    json:{"name":"test"},
                    headers:{"X-Auth-Token":"123"}
                },function(error,response,body){
                    response.statusCode.should.equal(401)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/clusters",
                        method:"POST",
                        json:{"name":"test"},
                        headers:{"X-Auth-Token":"user123"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        done()
                    })

                })
            })
        })

        it("Should validate tenant on topic creation",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/clusters",
                method:"POST",
                json:{"name":"test",
                      "nodes":[{
                        "name":"redis1", 
                        "config":{
                            "host":"127.0.0.1",
                            "port":6379,
                            "status":"UP"
                         }
                    }]},
                headers:{"X-Auth-Token":"user123"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics",
                    method:"POST",
                    json:{"group":"1234","name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(401)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/topics",
                        method:"POST",
                        json:{"group":"1234","name":"test"},
                        headers:{"X-Auth-Token":"someone"}
                    },function(error,response,body){
                        response.statusCode.should.equal(401)
                        request({
                            url:"http://127.0.0.1:8080/bigqueue/topics",
                            method:"POST",
                            json:{"group":"1234","name":"test"},
                            headers:{"X-Auth-Token":"user123"}
                        },function(error,response,body){
                            response.statusCode.should.equal(201)
                            done()
                        })
                    })
                })
            })
        })
        it("Should validate tenant on consumer creation",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/clusters",
                method:"POST",
                json:{"name":"test",
                      "nodes":[{
                        "name":"redis1", 
                        "config":{
                            "host":"127.0.0.1",
                            "port":6379,
                            "status":"UP"
                            }
                        }]
                    },
                headers:{"X-Auth-Token":"user123"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics",
                    method:"POST",
                    json:{"group":"someone","name":"test"},
                    headers:{"X-Auth-Token":"someone"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/topics/someone-test/consumers",
                        method:"POST",
                        json:{"group":"1234","name":"test"},
                    },function(error,response,body){
                        response.statusCode.should.equal(401)
                        request({
                            url:"http://127.0.0.1:8080/bigqueue/topics/someone-test/consumers",
                            method:"POST",
                            json:{"group":"1234","name":"test"},
                            headers:{"X-Auth-Token":"someone"}
                        },function(error,response,body){
                            response.statusCode.should.equal(401)
                            request({
                                url:"http://127.0.0.1:8080/bigqueue/topics/someone-test/consumers",
                                method:"POST",
                                json:{"group":"1234","name":"test"},
                                headers:{"X-Auth-Token":"user123"}
                            },function(error,response,body){
                                response.statusCode.should.equal(201)
                                done()
                            })
                        })
                    })
                })
            })
        })
    })
})
