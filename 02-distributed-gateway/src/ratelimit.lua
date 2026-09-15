-- ATOMIC TOKEN BUCKET
-- ===================
-- Runs INSIDE Redis. Redis executes scripts single-threaded, so nothing else
-- touches these keys mid-script. That atomicity is the entire reason this is
-- Lua and not application code.
--
-- The race this prevents (the naive version in app code):
--     tokens = await redis.get(key)     <-- 50 instances all read "1"
--     if (tokens >= 1)                  <-- all 50 agree there's a token
--     await redis.set(key, tokens - 1)  <-- all 50 take the same token
-- One token, handed out 50 times.
--
-- NO TIMER ANYWHERE: tokens are derived from elapsed time rather than
-- incremented by a background job. Nothing to drift, nothing to restart,
-- and only Redis's clock is ever read.
--
-- KEYS[1] = tokens key, KEYS[2] = last-refill timestamp key
-- Both use a {hash tag} so Redis Cluster puts them in the SAME slot —
-- cross-slot scripts are rejected outright.
--
-- ARGV[1] = capacity, ARGV[2] = refill rate/sec, ARGV[3] = now (ms)

local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])

local tokens = tonumber(redis.call('GET', KEYS[1]))
local last   = tonumber(redis.call('GET', KEYS[2]))

-- Cold start: full bucket.
if tokens == nil then
  tokens = capacity
  last   = now
end

-- Lazy refill — this replaces setInterval entirely.
local elapsed = math.max(0, now - last) / 1000.0
tokens = math.min(capacity, tokens + (elapsed * rate))

local granted = 0
if tokens >= 1 then
  tokens = tokens - 1
  granted = 1
end

-- TTL is generous: the bucket self-heals from a full state, so losing these
-- keys costs nothing. That disposability is precisely why Redis beats Raft
-- here — we do not want durability, we want speed.
redis.call('SET', KEYS[1], tokens, 'PX', 60000)
redis.call('SET', KEYS[2], now,    'PX', 60000)

return granted
