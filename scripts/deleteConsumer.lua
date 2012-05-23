local topic = ARGV[1]
local consumerGroup = ARGV[2]

local topicKey = "topics:"..topic
local topicConsumersKey = topicKey..":consumers"
local consumerKey = topicKey..":consumers:"..consumerGroup
local lastPointer = consumerKey..":last"

local res = redis.call("srem",topicConsumersKey,consumerGroup)

if not (tonumber(res) == 1) then
    return {err="Consumer ["..consumerGroup.."] not found for topic ["..topic.."]"}
end

redis.call("del",lastPointer)
