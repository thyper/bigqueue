local journalName = "journals:"..ARGV[1]
local idFrom = tonumber(ARGV[2])

local journalHead = journalName..":head"
-- Get the journal head
local head = tonumber(redis.call("get",journalHead))
if not head then
    return {err="Journal head not found for ["..journalName.."]"}
end

-- If the required from is grather than the head this journal has not the id's
if head < idFrom then
    return {err="Head bigest than the if required"}
end

local msgs = {}

for i = idFrom, head, 1 do
    local msg = redis.call("hgetall",journalName..":messages:"..i)
    if table.getn(msg) > 0 then 
        table.insert(msgs, msg)
    end
end

return msgs
