local topic = ARGV[1]
local ttl = ARGV[2]

if not (ttl == nil) and (type(ttl) == "string") then
    ttl = tonumber(ttl)
end

if ttl == nil or ttl <= 0 then
    -- Default ttl 3 days
    ttl = 3*24*60*60
end

local exist = redis.call("sismember","topics",topic)

if exist == 1 then
    return {err="Topic ["..topic.."] already exist"}
end

redis.call("set","topics:"..topic..":ttl",ttl)
redis.call("sadd","topics",topic)
return
