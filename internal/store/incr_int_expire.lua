local v = redis.call('INCRBY', KEYS[1], ARGV[1])
local expireAt = tonumber(ARGV[2])
local now = tonumber(redis.call('TIME')[1])
if expireAt > now then
  redis.call('EXPIREAT', KEYS[1], expireAt)
end
return v
