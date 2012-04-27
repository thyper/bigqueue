local topic = ARGV[1]
local consumer = ARGV[2]

local topicKey="topics:"..topic
local topicHead="topics:"..topic..":head"
local consumerKey=topicKey..":consumers:"..consumer
local lastPointer = consumerKey..":last"
local failsList = consumerKey..":fails"
local processingList = consumerKey..":processing"

local head = redis.call("get",topicHead)
if head == nil then
    return {err="Head for topic ["..topic.."] not found"}
end

if not head then
    head = 0
end

local lastId = redis.call("get",lastPointer)
if not lastId then
    return {err="Last pointer for consumer ["..consumer.."] of topic ["..topic.."] not found"}
end

-- The +1 reprecent the difference between the lastId and the head when all messages are readed
local lag = head - lastId + 1
local processing = redis.call("zcard",processingList)
local fails = redis.call("llen",failsList)
return {"lag",lag,"processing",processing,"fails",fails} 
