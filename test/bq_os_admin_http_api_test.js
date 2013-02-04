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
        "adminRoleId":3,
        "defaultCluster":"test"
    }
    
    var httpConfig = {
        "admConfig":admConfig,
        "port":8081,
        "maxTtl":500,
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

    after(function(done){
        zk.close()
        process.nextTick(function(){
            done()
        })
    })

    var api

    beforeEach(function(){
        api = httpApi.startup(httpConfig)
    })
    afterEach(function(done){
        api.shutdown()
        process.nextTick(function(){
            done()
        })
   })


    describe("Cluster Admin",function(){
        it("should enable to create new clusters",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"GET",
                json:true
            },function(error,response,body){
                should.not.exist(error)
                response.statusCode.should.equal(200)
                body.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8081/clusters",
                    method:"POST",
                    json:{"name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/clusters",
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
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.cluster.should.equal("test")
                    done()
                })
            })
        })

        it("should support add nodes",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.nodes.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test/nodes",
                        method:"POST",
                        json:{"name":"test1",config:{"host":"127.0.0.1","port":6379}}
                    },function(err,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/clusters/test",
                            method:"GET",
                            json:true
                        },function(err,response,body){
                            response.statusCode.should.equal(200)
                            body.nodes.should.have.length(1)
                            body.nodes[0].host.should.equal("127.0.0.1")
                            body.nodes[0].port.should.equal(6379)
                            body.nodes[0].host.should.equal("127.0.0.1")
                            done()
                        })
                    })

                })
            })
        })
        it("should support modify nodes",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.nodes.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test/nodes",
                        method:"POST",
                        json:{"name":"test1",config:{"host":"127.0.0.1","port":6379}}
                    },function(err,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/clusters/test",
                            method:"GET",
                            json:true
                        },function(err,response,body){
                            response.statusCode.should.equal(200)
                            body.nodes.should.have.length(1)
                            body.nodes[0].host.should.equal("127.0.0.1")
                            body.nodes[0].port.should.equal(6379)
                            request({
                                url:"http://127.0.0.1:8081/clusters/test/nodes/test1",
                                method:"PUT",
                                json:{config:{"host":"localhost","port":6379}}
                            },function(err,response,body){
                                response.statusCode.should.equal(200)
                                request({
                                    url:"http://127.0.0.1:8081/clusters/test",
                                    method:"GET",
                                    json:true
                                },function(err,response,body){
                                    response.statusCode.should.equal(200)
                                    body.nodes.should.have.length(1)
                                    body.nodes[0].host.should.equal("localhost")
                                    body.nodes[0].port.should.equal(6379)
                                    done()
                                })
                            })
                        })
                    })
                })
            })
        })
        it("should support add endpoints",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.endpoints.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test/endpoints",
                        method:"POST",
                        json:{"name":"test1",config:{"host":"127.0.0.1","port":6379}}
                    },function(err,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/clusters/test",
                            method:"GET",
                            json:true
                        },function(err,response,body){
                            response.statusCode.should.equal(200)
                            body.endpoints.should.have.length(1)
                            body.endpoints[0].host.should.equal("127.0.0.1")
                            body.endpoints[0].port.should.equal(6379)
                            done()
                        })
                    })

                })
            })
        })
        it("Should enable get info for nodes and journals",function(done){
            request({
                    url:"http://127.0.0.1:8081/clusters",
                    method:"POST",
                    json:{"name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test/nodes",
                        method:"POST",
                        json:{"id":"test1","name":"test1",config:{"host":"127.0.0.1","port":6379}}
                    },function(err,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/clusters/test/journals",
                            method:"POST",
                            json:{"id":"j1","name":"j1",config:{"host":"127.0.0.1","port":6379}}
                        },function(err,response,body){
                            response.statusCode.should.equal(201)
                            request({
                                url:"http://127.0.0.1:8081/clusters/test/nodes/test1",
                                method:"GET",
                                json:true
                            },function(error,response,body){
                                response.statusCode.should.equal(200)
                                body.host.should.equal("127.0.0.1")
                                body.port.should.equal(6379)
                                 request({
                                    url:"http://127.0.0.1:8081/clusters/test/journals/j1",
                                    method:"GET",
                                    json:true
                                },function(error,response,body){
                                    response.statusCode.should.equal(200)
                                    body.host.should.equal("127.0.0.1")
                                    body.port.should.equal(6379)
                                    done()
                                })
                            })
                        })
                    })
                })
        })

        it("should support delete endpoints")
        it("should support delete journals")
        it("should support add journals",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.journals.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test/journals",
                        method:"POST",
                        json:{"name":"test1",config:{"host":"127.0.0.1","port":6379}}
                    },function(err,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/clusters/test",
                            method:"GET",
                            json:true
                        },function(err,response,body){
                            response.statusCode.should.equal(200)
                            body.journals.should.have.length(1)
                            body.journals[0].host.should.equal("127.0.0.1")
                            body.journals[0].port.should.equal(6379)
                            done()
                        })
                    })

                })
            })
        })
        it("should support cluster deletes")

    })
    
    describe("Topics and consumers",function(){
        beforeEach(function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
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
                    url:"http://127.0.0.1:8081/clusters",
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
                url:"http://127.0.0.1:8081/clusters/test",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(200)
                body.topics.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenantId":"1234","name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test",
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
                url:"http://127.0.0.1:8081/clusters/test2",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(200)
                body.topics.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenantId":"1234","name":"test","cluster":"test2"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test2",
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
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenantId":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.consumers.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/topics/1234-test/consumers",
                        method:"POST",
                        json:{"tenantId":"456","name":"test-consumer"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/topics/1234-test",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.consumers.should.have.length(1)
                            body.consumers[0].consumer_id.should.equal("456-test-consumer")
                            done()
                        })
                    })
                })
            })
        })
        it("should support list all topics of a tenantId",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenantId":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics?tenantId=1234",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.length(1)
                    body.should.include("1234-test")
                     request({
                        url:"http://127.0.0.1:8081/topics",
                        method:"POST",
                        json:{"tenantId":"1234","name":"test1"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/topics?tenantId=1234",
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
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenantId":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                body.should.have.property("topic_id","1234-test")
                body.should.have.property("ttl")
                body.should.have.property("cluster")
                body.should.have.property("endpoints")
                body.should.have.property("consumers")
                done()
            })

        })
        it("should get all consumer data on create",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenantId":"1234","name":"test"}
            },function(error,response,body){
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test/consumers",
                    method:"POST",
                    json:{"tenantId":"456","name":"test1"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    body.should.have.property("topic_id","1234-test")
                    body.should.have.property("consumer_id","456-test1")
                    body.should.have.property("ttl")
                    body.should.have.property("cluster")
                    body.should.have.property("endpoints")
                    done()
                })
            })

        })
        
        it("should get all information about a topic",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenantId":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.property("topic_id","1234-test")
                    body.should.have.property("ttl")
                    body.should.have.property("cluster")
                    body.should.have.property("endpoints")
                    body.should.have.property("consumers")
                    done()
                })
            })

        })
        it("should get all information about a consumer",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenantId":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test/consumers",
                    method:"POST",
                    json:{"tenantId":"456","name":"test1"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/topics/1234-test/consumers/456-test1",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        response.statusCode.should.equal(200)
                        body.should.have.property("topic_id","1234-test")
                        body.should.have.property("consumer_id","456-test1")
                        body.should.have.property("ttl")
                        body.should.have.property("cluster")
                        body.should.have.property("endpoints")
                        done()
                    })
                })
            })
        })
        
        it("should support list all consumer of a topic",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenantId":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test/consumers",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/topics/1234-test/consumers",
                        method:"POST",
                        json:{"tenantId":"456","name":"test"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/topics/1234-test/consumers",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.should.have.length(1)
                            body[0].consumer_id.should.equal("456-test")
                            done()
                        })
                    })
                })
            })

        })

        it("should support topic deletes",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenantId":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics?tenantId=1234",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.length(1)
                    body.should.include("1234-test")
                    request({
                        url:"http://127.0.0.1:8081/topics/1234-test",
                        method:"DELETE",
                    },function(error,response,body){
                        response.statusCode.should.equal(204)
                        request({
                            url:"http://127.0.0.1:8081/topics?tenantId=1234",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.should.have.length(0)
                            done()
                        }) 
                    })
                })
            })
        })

        it("should support consumers delete",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenantId":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test/consumers",
                    method:"POST",
                    json:{"tenantId":"1234","name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/topics/1234-test",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        body.consumers.length.should.equal(1)
                        request({
                            url:"http://127.0.0.1:8081/topics/1234-test/consumers/1234-test",
                            method:"DELETE",
                        },function(error,response,body){
                            response.statusCode.should.equal(204)
                            request({
                                url:"http://127.0.0.1:8081/topics/1234-test",
                                method:"GET",
                                json:true
                            },function(error,response,body){
                                body.consumers.length.should.equal(0)
                                done()
                            }) 
                        })
                    }) 
                })
            })
        })
        it("should support consumers reset",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenantId":"1234","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test/consumers",
                    method:"POST",
                    json:{"tenantId":"1234","name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/topics/1234-test",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        body.consumers.length.should.equal(1)
                        request({
                            url:"http://127.0.0.1:8081/topics/1234-test/consumers/1234-test",
                            method:"PUT",
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            done()
                        })
                    }) 
                })
            })
        })
    })

    describe("Limits",function(done){
        it("should limit the ttl time to the default ttl",function(done){
            request({
                    url:"http://127.0.0.1:8081/clusters",
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
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenantId":"1234","name":"test","ttl":500}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/topics",
                        method:"POST",
                        json:{"tenantId":"1234","name":"test","ttl":501}
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
                                "tenant":
                                     {
                                         "id": "1", 
                                         "name": "1234"
                                     }
                                
                            },
                            "user":{
                                "roles": [
                                    {
                                        "id": "3", 
                                        "name": "Admin", 
                                        "tenantId": "1"
                                    }
                                ]
                            }
                        }

                    },200)
                }
                if(req.params.token === "user345" && req.headers["x-auth-token"] === "admin"){
                    return res.json({
                        "access": {
                            "token":{
                                "tenant":
                                     {
                                         "id": "3", 
                                         "name": "345"
                                     }
                                
                            },
                            "user":{
                            }
                        }
                
                    },200)
                }

                if(req.params.token === "someone" && req.headers["x-auth-token"] === "admin"){
                    return res.json({
                        "access": {
                            "token":{
                                "tenant":
                                     {
                                         "id": "2", 
                                         "name": "someone"
                                     }
                                
                            },
                            "user":{}
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
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(401)
                request({
                    url:"http://127.0.0.1:8081/clusters",
                    method:"POST",
                    json:{"name":"test"},
                    headers:{"X-Auth-Token":"123"}
                },function(error,response,body){
                    response.statusCode.should.equal(401)
                    request({
                        url:"http://127.0.0.1:8081/clusters",
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
                url:"http://127.0.0.1:8081/clusters",
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
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenantId":"1","name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(401)
                    request({
                        url:"http://127.0.0.1:8081/topics",
                        method:"POST",
                        json:{"tenantId":"1","name":"test"},
                        headers:{"X-Auth-Token":"someone"}
                    },function(error,response,body){
                        response.statusCode.should.equal(401)
                        request({
                            url:"http://127.0.0.1:8081/topics",
                            method:"POST",
                            json:{"tenantId":"1","name":"test"},
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
                url:"http://127.0.0.1:8081/clusters",
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
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenantId":"2","name":"test"},
                    headers:{"X-Auth-Token":"someone"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/topics/2-test/consumers",
                        method:"POST",
                        json:{"tenantId":"1234","name":"test"},
                    },function(error,response,body){
                        response.statusCode.should.equal(401)
                        request({
                            url:"http://127.0.0.1:8081/topics/2-test/consumers",
                            method:"POST",
                            json:{"tenantId":"1","name":"test"},
                            headers:{"X-Auth-Token":"someone"}
                        },function(error,response,body){
                            response.statusCode.should.equal(401)
                            request({
                                url:"http://127.0.0.1:8081/topics/2-test/consumers",
                                method:"POST",
                                json:{"tenantId":"2","name":"test"},
                                headers:{"X-Auth-Token":"someone"}
                            },function(error,response,body){
                                response.statusCode.should.equal(201)
                                done()
                            })
                        })
                    })
                })
            })
        })
        
        it("should only create consumer into a topic if these has the same tenant",function(done){
           request({
                url:"http://127.0.0.1:8081/clusters",
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
               request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenantId":"2","name":"test"},
                    headers:{"X-Auth-Token":"someone"}
                },function(error,response,body){
                    request({
                        url:"http://127.0.0.1:8081/topics/2-test/consumers",
                        method:"POST",
                        json:{"tenantId":"3","name":"test"},
                        headers:{"X-Auth-Token":"user345"}
                    },function(error,response,body){
                        response.statusCode.should.equal(401)
                        request({
                            url:"http://127.0.0.1:8081/topics/2-test/consumers",
                            method:"POST",
                            json:{"tenantId":"2","name":"test"},
                            headers:{"X-Auth-Token":"someone"}
                        },function(error,response,body){
                            response.statusCode.should.equal(201)
                            done()
                        })
                    })
                })
            })
        })

        it("should only create topic without the specific tenant if the user is admin",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
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
               request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenantId":"user345","name":"test"},
                    headers:{"X-Auth-Token":"someone"}
                },function(error,response,body){
                   response.statusCode.should.equal(401)
                   request({
                        url:"http://127.0.0.1:8081/topics",
                        method:"POST",
                        json:{"tenantId":"user345","name":"test"},
                        headers:{"X-Auth-Token":"user123"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        done()
                    })
                })
            })

        })

        it("should only create consumer into a topic if is not the has the tenant, if the user  has the admin role",function(done){
           request({
                url:"http://127.0.0.1:8081/clusters",
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
               request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenantId":"2","name":"test"},
                    headers:{"X-Auth-Token":"someone"}
                },function(error,response,body){
                    request({
                        url:"http://127.0.0.1:8081/topics/2-test/consumers",
                        method:"POST",
                        json:{"tenantId":"3","name":"test"},
                        headers:{"X-Auth-Token":"user345"}
                    },function(error,response,body){
                        response.statusCode.should.equal(401)
                        request({
                            url:"http://127.0.0.1:8081/topics/2-test/consumers",
                            method:"POST",
                            json:{"tenantId":"1","name":"test"},
                            headers:{"X-Auth-Token":"user123"}
                        },function(error,response,body){
                            response.statusCode.should.equal(201)
                            done()
                        })
                    })
                })
            })

        })
        it("should check topic owner before delete topic",function(done){
           request({
                url:"http://127.0.0.1:8081/clusters",
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
                request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenantId":"1","name":"test"},
                    headers:{"X-Auth-Token":"user123"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/topics/1-test",
                        method:"DELETE",
                    },function(error,response,body){
                        response.statusCode.should.equal(401)
                        request({
                            url:"http://127.0.0.1:8081/topics/1-test",
                            method:"DELETE",
                            headers:{"X-Auth-Token":"someone"}
                        },function(error,response,body){
                            response.statusCode.should.equal(401)
                            request({
                                url:"http://127.0.0.1:8081/topics/1-test",
                                method:"DELETE",
                                headers:{"X-Auth-Token":"user123"}
                            },function(error,response,body){
                                response.statusCode.should.equal(204)
                                done()
                            })
                        })
                    })
                })
            })

        })
        it("should check consumer owner before delete",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
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
                request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenantId":"1","name":"test"},
                    headers:{"X-Auth-Token":"user123"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/topics/1-test/consumers",
                        method:"POST",
                        json:{"tenantId":"1","name":"test"},
                        headers:{"X-Auth-Token":"user123"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/topics/1-test/consumers/1-test",
                            method:"DELETE",
                        },function(error,response,body){
                            response.statusCode.should.equal(401)
                            request({
                                url:"http://127.0.0.1:8081/topics/1-test/consumers/1-test",
                                method:"DELETE",
                                headers:{"X-Auth-Token":"someone"}
                            },function(error,response,body){
                                response.statusCode.should.equal(401)
                                request({
                                    url:"http://127.0.0.1:8081/topics/1-test/consumers/1-test",
                                    method:"DELETE",
                                    headers:{"X-Auth-Token":"user123"}
                                },function(error,response,body){
                                    response.statusCode.should.equal(204)
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
