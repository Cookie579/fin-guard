-- KEYS[1] = "rl:<api>:<idx>"
-- ARGV[1]=now(epoch s), ARGV[2]=status(int), ARGV[3]=retry_after_sec(int)

local now    = tonumber(ARGV[1])
local status = tonumber(ARGV[2]) or 0
local retry  = tonumber(ARGV[3]) or 0

-- Decrement inflight if > 0
local infl = tonumber(redis.call('HGET', KEYS[1], 'inflight')) or 0
if infl > 0 then
  redis.call('HINCRBY', KEYS[1], 'inflight', -1)
end

if status == 429 then
  -- backoff/cooldown
  local back = tonumber(redis.call('HGET', KEYS[1], 'backoff')) or 1
  if retry > 0 then
    back = retry
  else
    back = back * 2
    if back > 30 then back = 30 end
  end
  redis.call('HMSET', KEYS[1], 'frozen_until', now + back, 'backoff', back)

  -- penalty: subtract one token if any
  local tok = tonumber(redis.call('HGET', KEYS[1], 'tokens')) or 0
  if tok > 0 then
    redis.call('HSET', KEYS[1], 'tokens', tok - 1)
  end
else
  -- success: reset backoff
  redis.call('HSET', KEYS[1], 'backoff', 1)
end

return 1
