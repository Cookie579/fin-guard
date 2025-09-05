-- KEYS[1] = "rl:<api>:<idx>"
-- ARGV[1]=now(epoch s), ARGV[2]=rate(tokens/sec), ARGV[3]=burst, ARGV[4]=max_inflight

local now   = tonumber(ARGV[1])
local rate  = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local maxif = tonumber(ARGV[4])

local tokens  = tonumber(redis.call('HGET', KEYS[1], 'tokens')) or burst
local ts      = tonumber(redis.call('HGET', KEYS[1], 'ts')) or now
local frozen  = tonumber(redis.call('HGET', KEYS[1], 'frozen_until')) or 0
local infl    = tonumber(redis.call('HGET', KEYS[1], 'inflight')) or 0

-- Respect cooldown after 429
if now < frozen then
  return {0, frozen - now}
end

-- Refill bucket
local delta = now - ts
if delta < 0 then delta = 0 end
tokens = math.min(burst, tokens + delta * rate)

-- Not enough tokens? Tell client how long to wait (seconds)
if tokens < 1 then
  local wait = math.ceil((1 - tokens) / rate)
  redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
  return {0, wait}
end

-- Inflight guard
if infl >= maxif then
  return {0, 1}
end

-- Consume 1 token and bump inflight
tokens = tokens - 1
infl   = infl + 1
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now, 'inflight', infl)

return {1, 0}
