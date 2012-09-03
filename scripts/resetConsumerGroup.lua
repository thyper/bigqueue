local topic = ARGV[1]
local consumerGroup = ARGV[2]

local topicKey = "topics:"..topic
local topicConsumersKey = topicKey..":consumers"
local topicHead = "topics:"..topic..":head"
local consumerKey = topicKey..":consumers:"..consumerGroup
local lastPointer = consumerKey..":last"
local failsList = consumerKey..":fails"
local processingList = consumerKey..":processing"


local existTopic = redis.call("sismember","topics",topic)
if existTopic == 0 then
    return {err="Topic ["..topic.."] doesn't exist"}
end

local existConsumer = redis.call("sismember", topicConsumersKey, consumerGroup) 
if existConsumer == 0 then
    return {err="Consumer ["..consumerGroup.."] for topic ["..topic.."] does not exist"}
end

local head = redis.call("get",topicHead)
if not head then
    head = 1
end

redis.call("set",lastPointer,head)
redis.call("del",failsList)
redis.call("del",processingList)

return consumerKey 
