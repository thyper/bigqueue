local topic = ARGV[1]
local topicKey = "topics:"..topic
local topicHeadKey = "topics:"..topic..":head"
local topicTtlKey = "topics:"..topic..":ttl"
local topicConsumersKey = topicKey..":consumers"

local exists = redis.call("sismember","topics",topic)

if tonumber(exists) == 0 then
    return {err="Topic ["..topic.."] not found"}
end

local consumers = redis.call("scard",topicConsumersKey)

if tonumber(consumers) > 0 then
    return {err="Topic ["..topic.."] contains consumers"}
end

redis.call("srem","topics",topic)
redis.call("del",topicHeadKey)
redis.call("del",topicTtlKey)

