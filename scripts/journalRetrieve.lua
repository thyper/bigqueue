local journalName = "journals:"..ARGV[1]..":"..ARGV[2]
local idFrom = tonumber(ARGV[3])

local journalHead = journalName..":head"
-- Get the journal head
local headExists = redis.call("exists",journalHead)
local head = tonumber(redis.call("get",journalHead))
if headExists == 0 or not head then
        return {}
end

-- If the required from is grather than the head this journal has not the id's
if head < idFrom then
    return {}
end

local diff = head - idFrom
if diff > 10000 then
    return {err="Can not return because the diff from head is more than 10000 messages"}
end

local msgs = {}

for i = idFrom, head, 1 do
    local msg = redis.call("hgetall",journalName..":messages:"..i)
    if table.getn(msg) > 0 then 
        table.insert(msgs, msg)
    end
end

return msgs
