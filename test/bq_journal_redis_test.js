var should = require('should'),
    redis = require('simple_redis_client'),
    bj = require('../lib/bq_journal_client_redis.js'),
    log = require("node-logging")

describe("Big Queue Redis Journal Client",function(){
    
    var redisClient
    var redisConf= {host:"127.0.0.1",port:6379}
    var journal 
    before(function(done){
        log.setLevel("critical")
        journal = bj.createJournalClient(redisConf)
        journal.on("ready",function(){
            redisClient = redis.createClient(redisConf.port,redisConf.host,{"return_buffers":false})
            redisClient.execute("on","ready",function(){
                done()
            })
        })
    })    

    beforeEach(function(done){
        redisClient.execute("flushall",function(data,err){
            done()
        })
    })    

    describe("#Journal Write",function(){
        it("should receive a message and store into a named journal",function(done){
            journal.write("testJournal","testTopic",1,{msg:"testMessage"},undefined,function(err){
                should.not.exist(err)
                redisClient.execute("get","journals:testJournal:testTopic:head",function(err,data){
                    should.not.exist(err)
                    should.exist(data)
                    data.should.equal("1")
                    redisClient.execute("hgetall","journals:testJournal:testTopic:messages:1",function(err,data){
                        should.not.exist(err)
                        should.exist(data)
                        data[0].should.equal("msg")
                        data[1].should.equal("testMessage")
                        done()
                    })
                })
            })
        })
        it("should return an error if the message can't be stored",function(done){
            journal.write("testJournal","testTopic",1,"text is not json",undefined,function(err){
                should.exist(err)
                done()
            })
        })
    })

    describe("#Journal retrive data",function(){
        beforeEach(function(done){
            journal.write("testJournal","testTopic",1,{msg:"testMessage"},undefined,function(err){
                journal.write("testJournal","testTopic",2,{msg:"testMessage"},undefined,function(err){
                    journal.write("testJournal","testTopic",3,{msg:"testMessage"},undefined,function(err){
                        should.not.exist(err)
                        done()
                    })
                })
            })
        })
        it("should enable to get the published messages from an id to the end",function(done){
            journal.retrieveMessages("testJournal","testTopic",1,function(err,data){
                should.not.exist(err)
                should.exist(data)
                data.should.have.length(3)
                for(var d in data){
                    data[d].should.have.property("msg")
                    data[d].msg.should.equal("testMessage")
                }
                done()
            })
        })
        it("should ignore unexistent messages id's",function(done){
            journal.write("testJournal","testTopic",10,{msg:"testMessage"},undefined,function(err){
                redisClient.execute("get","journals:testJournal:testTopic:head",function(err,data){
                    should.not.exist(err)
                    data.should.equal(""+10)
                    journal.retrieveMessages("testJournal","testTopic",1,function(err,data){
                        should.not.exist(err)
                        should.exist(data)
                        data.should.have.length(4)
                        for(var d in data){
                            data[d].should.have.property("msg")
                            data[d].msg.should.equal("testMessage")
                        }
                        done()
                    })
                })
            })
        })
        it("should return void list if the last message sent is higher than the last message stored",function(done){
            journal.retrieveMessages("testJournal","testTopic",5,function(err,data){
                should.not.exist(err)
                data.should.have.length(0)
                done()
            })
        })
        it("should return void list if the id required is 0 and doesn't exist",function(done){
             journal.retrieveMessages("testJournal","testunexistentTopic",0,function(err,data){
                should.not.exist(err)
                data.should.have.length(0)
                done()
            })

        })
    })

})
