local journalName = "journals:"..ARGV[1]
local id = tonumber(ARGV[2])
local jsonMessage = ARGV[3]
local ttl = ARGV[4]

local journalHead = journalName..":head"
local journalMessageKey = journalName..":messages:"..id

if ttl == nil or ttl == "undefined" or tonumber(ttl)<=0 then
    -- six hours
    ttl = 21600
end

-- Get the las id wrote
local head = redis.call("get",journalHead)
if head == nil or not head then
    head = 0
else
    head = tonumber(head)
end

--Check if the new id is the last and write it
if id > head then
    redis.call("set",journalHead,id)
end

-- Transform json to hash table
message = cjson.decode(jsonMessage)

for k,v in pairs(message) do
    redis.call('hmset',journalMessageKey,k,v) 
end

-- Set the expire
redis.call('expire',journalMessageKey,ttl)

return {ok="wrote"} 
