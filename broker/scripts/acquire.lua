-- KEYS[1] = "rl:<api>:<idx>"
-- ARGV = now, rate_tokens_per_sec, burst, max_inflight
local now   = tonumber(ARGV[1])
local rate  = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local maxif = tonumber(ARGV[4])

local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens')) or burst
local ts     = tonumber(redis.call('HGET', KEYS[1], 'ts')) or now
local frozen = tonumber(redis.call('HGET', KEYS[1], 'frozen_until')) or 0
local infl   = tonumber(redis.call('HGET', KEYS[1], 'inflight')) or 0

-- still in cooldown?
if now < frozen then
  return {0, frozen - now}
end

-- refill
local delta = math.max(0, now - ts)
tokens = math.min(burst, tokens + delta * rate)

-- capacity?
if tokens < 1 then
  local wait = math.ceil((1 - tokens) / rate)
  redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
  return {0, wait}
end

-- inflight guard
if infl >= maxif then
  return {0, 1}
end

-- consume 1
tokens = tokens - 1
infl   = infl + 1
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now, 'inflight', infl)
return {1, 0}
