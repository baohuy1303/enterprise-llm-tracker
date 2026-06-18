-- Hot-path event recorder: collapses the per-event Redis writes into ONE round
-- trip (was 3 for a cost event: cost:today, cost:month, last-seen). Atomic.
-- Mirrors the semantics of the old incr_float_expire.lua / incr_int_expire.lua:
-- INCR the counter, then EXPIREAT only if the expiry is still in the future.
--
-- KEYS[1] = cost:today   KEYS[2] = cost:month   KEYS[3] = tokens:today
-- KEYS[4] = last-otel
-- ARGV[1] = cost (float)  ARGV[2] = eod (unix)   ARGV[3] = eom (unix)
-- ARGV[4] = tokens (int)  ARGV[5] = last-otel value (RFC3339)
local now = tonumber(redis.call('TIME')[1])
local eod = tonumber(ARGV[2])
local eom = tonumber(ARGV[3])

local cost = tonumber(ARGV[1])
if cost and cost > 0 then
  redis.call('INCRBYFLOAT', KEYS[1], ARGV[1])
  if eod > now then redis.call('EXPIREAT', KEYS[1], eod) end
  redis.call('INCRBYFLOAT', KEYS[2], ARGV[1])
  if eom > now then redis.call('EXPIREAT', KEYS[2], eom) end
end

local tokens = tonumber(ARGV[4])
if tokens and tokens > 0 then
  redis.call('INCRBY', KEYS[3], ARGV[4])
  if eod > now then redis.call('EXPIREAT', KEYS[3], eod) end
end

redis.call('SET', KEYS[4], ARGV[5])
return 1
