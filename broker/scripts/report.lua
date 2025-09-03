-- KEYS[1] = "rl:<api>:<idx>"
-- ARGV = now, status, retry_after_sec
local now   = tonumber(ARGV[1])
local st    = tonumber(ARGV[2]) or 0
local ra    = tonumber(ARGV[3]) or 0

local infl = tonumber(redis.call('HGET', KEYS[1], 'inflight')) or 0
if infl > 0 then
  infl = infl - 1
  redis.call('HSET', KEYS[1], 'inflight', infl)
end

if st == 429 then
  local until = now + math.max(ra, 5) -- min 5s freeze
  redis.call('HSET', KEYS[1], 'frozen_until', until)
  -- optional penalty
  local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens')) or 0
  tokens = math.max(0, tokens - 1)
  redis.call('HSET', KEYS[1], 'tokens', tokens)
end

return {infl}
