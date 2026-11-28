"""Lua scripts the worker runs. Each one is a single atomic step in Redis."""

from pytaskq.core import queueing
from pytaskq.core.keys import fill_scripts
from pytaskq.core.redis_health import OOM_PROBE


# A job's record is gone while it ran (Redis was flushed or lost its data): write its basic fields back
# from the job itself, so its outcome isn't stored as a nameless half-record.
REBUILD_LUA = """
local function pytaskq_rebuild(hash, payload, now)
    if redis.call('EXISTS', hash) == 1 then return end
    local ok, job = pcall(cjson.decode, payload)
    if not ok or type(job) ~= 'table' then return end
    local function text(v, default)
        if type(v) == 'string' or type(v) == 'number' then return tostring(v) end
        return default
    end
    redis.call('HSET', hash, 'task_name', text(job['task_name'], '(unknown)'), 'queue', text(job['queue'], 'default'),
               'priority', text(job['priority'], 'default'), 'payload', payload, 'created_at', now,
               'note', 'Its record was missing when it finished (Redis lost its data); rebuilt from the job.')
end
"""

# Unused; kept for importers. KEYS: fence, task hash
FENCE_SCRIPT = """
local current = redis.call('get', KEYS[1])
if current == false or current == ARGV[1] then
    redis.call('hset', KEYS[2], 'task_id', ARGV[2], 'status', ARGV[3], 'result', ARGV[4])
    redis.call('expire', KEYS[2], tonumber(ARGV[5]) or 86400)
    return 1
else
    return 0
end
"""

# On timeout, invalidate this attempt, only if it still owns the job (else the
# sweeper gave it to another worker). Returns the new token, or -1.
TIMEOUT_FENCE_SCRIPT = OOM_PROBE + """
local current = redis.call('get', KEYS[1])
if current == false or current == ARGV[1] then
    local token = redis.call('incr', KEYS[1])
    redis.call('expire', KEYS[1], 86400)
    return token
end
return -1
"""

# Starts with OOM_PROBE: taking a job must work when Redis is full (finishing jobs frees memory).
# KEYS: processing list, the queue lists in order [, paused-queues hash]
# ARGV (optional): the queue name of each list; paused queues are skipped in the same step.
# Returns the job, or nil. A list key holding another type is skipped (so one bad key can't stop
# every queue); if nothing was found, the reply is {"PYTASKQ_WRONGTYPE", key, …} instead of nil.
PRIORITY_POP_SCRIPT = OOM_PROBE + """
local dest = KEYS[1]
local last, paused, skip, bad = #KEYS, nil, {}, nil
if #ARGV > 0 then
    paused, last = KEYS[#KEYS], #KEYS - 1
end
for i = 2, last do
    local name = ARGV[i - 1]
    if paused and skip[name] == nil then
        skip[name] = redis.call('HEXISTS', paused, name) == 1
    end
    if not (paused and skip[name]) then
        local val = redis.pcall('RPOPLPUSH', KEYS[i], dest)
        if type(val) == 'table' and val.err then
            if string.find(tostring(val.err), 'WRONGTYPE', 1, true) then
                bad = bad or {'PYTASKQ_WRONGTYPE'}
                table.insert(bad, KEYS[i])
            else
                return redis.error_reply(tostring(val.err))
            end
        elseif val then
            return val
        end
    end
end
return bad
"""

# Take up to N jobs in one round trip (a worker with several free slots far from Redis isn't held to one
# job per round trip). Same order and skipping as PRIORITY_POP_SCRIPT; starts with OOM_PROBE.
# KEYS: processing list, the queue lists in order [, paused-queues hash]
# ARGV: how many, then (optional) the queue name of each list
# Returns {n, job 1 … job n, then any list keys that hold another type (skipped)}.
PRIORITY_POP_MANY_SCRIPT = OOM_PROBE + """
local dest = KEYS[1]
local want = tonumber(ARGV[1])
local last, paused, skip = #KEYS, nil, {}
if #ARGV > 1 then
    paused, last = KEYS[#KEYS], #KEYS - 1
end
local got, bad = {}, {}
for i = 2, last do
    local name = ARGV[i]
    if paused and skip[name] == nil then
        skip[name] = redis.call('HEXISTS', paused, name) == 1
    end
    if not (paused and skip[name]) then
        while #got < want do
            local val = redis.pcall('RPOPLPUSH', KEYS[i], dest)
            if type(val) == 'table' and val.err then
                if string.find(tostring(val.err), 'WRONGTYPE', 1, true) then
                    table.insert(bad, KEYS[i])
                    break
                end
                return redis.error_reply(tostring(val.err))
            elseif val then
                table.insert(got, val)
            else
                break
            end
        end
    end
    if #got >= want then break end
end
local out = {#got}
for _, v in ipairs(got) do table.insert(out, v) end
for _, k in ipairs(bad) do table.insert(out, k) end
return out
"""

# Give up the leader lock on a clean stop, so another worker leads at once.  KEYS: lock   ARGV: worker id
LEADER_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""

# Take or renew the leader lock atomically.  KEYS: lock   ARGV: worker id, seconds
LEADER_SCRIPT = OOM_PROBE + """
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2])) then return 1 end
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 1
end
return 0
"""

# A job succeeded: save the result and take it off this worker's processing list.
# KEYS: processing list, fence, task hash, worker hash, completed counter, finished-jobs set
#       [, the job's cancel request: deleted]
# ARGV: payload, my fence token, strict (1 = a retry after a lost reply), task id, result,
#       finished_at, duration_ms, result_dropped, result TTL, jobs kept, task-key prefix
# Returns 1 saved, 0 stale attempt (a newer one owns the job), -1 nothing to do (already done);
# a PYTASKQ_WRONGTYPE error (nothing written) when a key holds another type.
COMPLETE_SCRIPT = queueing.KEEP_LUA + queueing.TYPE_LUA + REBUILD_LUA + """
local bad = pytaskq_need(KEYS[1], 'list') or pytaskq_need(KEYS[3], 'hash') or pytaskq_need(KEYS[6], 'zset')
if bad then return bad end
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
-- Already done by this call: a re-send after a lost reply.
if removed == 0 and (ARGV[3] == '1' or redis.call('HGET', KEYS[3], 'status') == 'Success') then return -1 end
local current = redis.call('GET', KEYS[2])
if current and current ~= ARGV[2] then
    redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
    return 0
end
pytaskq_rebuild(KEYS[3], ARGV[1], ARGV[6])
redis.call('HSET', KEYS[3], 'task_id', ARGV[4], 'status', 'Success', 'result', ARGV[5],
           'finished_at', ARGV[6], 'duration_ms', ARGV[7], 'error', '')
if ARGV[8] ~= '' then
    redis.call('HSET', KEYS[3], 'result_dropped', ARGV[8])
else
    redis.call('HDEL', KEYS[3], 'result_dropped')
end
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[9]))
pytaskq_keep(KEYS[6], ARGV[4], ARGV[6], tonumber(ARGV[10]), ARGV[11])
if KEYS[7] then redis.call('DEL', KEYS[7]) end
redis.pcall('INCR', KEYS[5])                       -- counters only: a broken one never loses a job
redis.pcall('HINCRBY', KEYS[4], 'processed', 1)
redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
return 1
"""

# A job failed: schedule its retry or dead-letter it, and take it off the processing list.
# KEYS: processing list, fence, task hash, dlq or delayed set, worker hash, failed counter, finished-jobs set
#       [, the job's cancel request: deleted on dead-letter, kept for a retry (which must not run)]
# ARGV: payload, my fence token, strict, "dead" | "retry", dlq entry or retry payload, retry due time,
#       hash TTL ("" = keep until finished), dead-letter limit, task id, now, jobs kept, task-key prefix,
#       then task hash fields (name, value, …)
# Returns 1 recorded, 2 recorded and old dead letters trimmed, 0 stale attempt, -1 already done;
# a PYTASKQ_WRONGTYPE error (nothing written) when a key holds another type.
FAIL_SCRIPT = queueing.KEEP_LUA + queueing.TYPE_LUA + REBUILD_LUA + """
local unpack = unpack or table.unpack
local bad = pytaskq_need(KEYS[1], 'list') or pytaskq_need(KEYS[3], 'hash')
    or pytaskq_need(KEYS[4], ARGV[4] == 'dead' and 'list' or 'zset') or pytaskq_need(KEYS[7], 'zset')
if bad then return bad end
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 then
    -- Already recorded by this call (a re-send after a lost reply): no second dead letter or retry.
    local st = redis.call('HGET', KEYS[3], 'status')
    if ARGV[3] == '1' or st == 'RetryScheduled' or st == 'DeadLetter' then return -1 end
end
local current = redis.call('GET', KEYS[2])
if current and current ~= ARGV[2] then
    redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
    return 0
end
pytaskq_rebuild(KEYS[3], ARGV[1], ARGV[10])
redis.pcall('INCR', KEYS[6])                       -- counters only: a broken one never loses a job
redis.pcall('HINCRBY', KEYS[5], 'failed', 1)
local trimmed = 0
if ARGV[4] == 'dead' then
    redis.call('LPUSH', KEYS[4], ARGV[5])
    local limit = tonumber(ARGV[8])
    if limit > 0 and redis.call('LLEN', KEYS[4]) > limit then
        redis.call('LTRIM', KEYS[4], 0, limit - 1)
        trimmed = 1
    end
    if KEYS[8] then redis.call('DEL', KEYS[8]) end
else
    redis.call('ZADD', KEYS[4], ARGV[6], ARGV[5])
end
redis.call('HSET', KEYS[3], unpack(ARGV, 13))
if ARGV[7] == '' then
    redis.call('PERSIST', KEYS[3])
else
    redis.call('EXPIRE', KEYS[3], tonumber(ARGV[7]))
    pytaskq_keep(KEYS[7], ARGV[9], ARGV[10], tonumber(ARGV[11]), ARGV[12])
end
redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
return 1 + trimmed
"""

# Put a job from a processing list back on its queue, only if it's still listed
# (a job that just finished is never queued again).
# KEYS: processing list, fence, target list, task hash, queue-name set, the queue's wake list
#       [, dlq, finished-jobs set, failed counter: only with a recovery limit]
# ARGV: payload as listed, new payload, fence value read before ("" = none), new fence, note, "front" | "back",
#       queue name [, recovery limit (0 = none), dead-letter entry, now, result TTL, dead-letter limit, jobs kept,
#       task-key prefix, error]
# With a recovery limit, a job that was running (status Processing) counts one more recovery; at the
# limit it is dead-lettered instead of put back (its worker stopped that many times while running it).
# Returns 1 put back, 2 was already finished (entry dropped), 3 dead-lettered (recovery limit), 0 not in the
# list, -2 fence changed (read again); a PYTASKQ_WRONGTYPE error (nothing written) when a key holds another type.
PUT_BACK_SCRIPT = queueing.WAKE_LUA + queueing.KEEP_LUA + queueing.TYPE_LUA + """
local limit = tonumber(ARGV[8] or '0') or 0
local bad = pytaskq_need(KEYS[1], 'list') or pytaskq_need(KEYS[3], 'list') or pytaskq_need(KEYS[4], 'hash')
    or pytaskq_need(KEYS[5], 'set')
if not bad and limit > 0 then bad = pytaskq_need(KEYS[7], 'list') or pytaskq_need(KEYS[8], 'zset') end
if bad then return bad end
local current = redis.call('GET', KEYS[2])
if (current or '') ~= ARGV[3] then return -2 end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then return 0 end
local status = redis.call('HGET', KEYS[4], 'status')
if status == 'Success' or status == 'DeadLetter' or status == 'Cancelled' or status == 'Failed' then
    return 2
end
if limit > 0 and status == 'Processing' then
    local n = redis.call('HINCRBY', KEYS[4], 'recoveries', 1)
    if n >= limit then
        redis.call('LPUSH', KEYS[7], ARGV[9])
        local dl = tonumber(ARGV[12])
        if dl > 0 and redis.call('LLEN', KEYS[7]) > dl then redis.call('LTRIM', KEYS[7], 0, dl - 1) end
        redis.call('HSET', KEYS[4], 'status', 'DeadLetter', 'finished_at', ARGV[10], 'error', ARGV[15],
                   'note', ARGV[5])
        redis.call('HDEL', KEYS[4], 'worker_id')
        redis.call('EXPIRE', KEYS[4], tonumber(ARGV[11]))
        local id = redis.call('HGET', KEYS[4], 'task_id')
        if id then pytaskq_keep(KEYS[8], id, ARGV[10], tonumber(ARGV[13]), ARGV[14]) end
        redis.pcall('INCR', KEYS[9])
        redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
        return 3
    end
end
redis.call('SET', KEYS[2], ARGV[4], 'EX', 86400)
if ARGV[6] == 'front' then
    redis.call('RPUSH', KEYS[3], ARGV[2])
else
    redis.call('LPUSH', KEYS[3], ARGV[2])
end
redis.call('SADD', KEYS[5], ARGV[7])
redis.pcall('LPUSH', KEYS[6], '1')                  -- the wake-up token is only a hint
redis.pcall('LTRIM', KEYS[6], 0, 99)
if status then
    redis.call('HSET', KEYS[4], 'status', 'Queued', 'payload', ARGV[2], 'note', ARGV[5])
    redis.call('HDEL', KEYS[4], 'started_at', 'worker_id')
    redis.call('PERSIST', KEYS[4])
end
redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
return 1
"""

# This worker lacks the job's task: send it back through the delayed set.
# KEYS: processing list, delayed set, task hash
# ARGV: payload as listed, new payload, due, note [, the field to write it to (default "note")]
HAND_BACK_SCRIPT = """
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then return 0 end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
redis.call('HSET', KEYS[3], 'status', 'Queued', 'payload', ARGV[2], ARGV[5] or 'note', ARGV[4])
redis.call('PERSIST', KEYS[3])
return 1
"""

# A cancelled job stopped while running. KEYS: processing list, fence, task hash, finished set, cancel request
# ARGV: payload as listed, fence token, retrying after a lost reply, task id, now, ttl, keep limit, task key
# prefix, note, duration (ms)
# Returns 1 recorded, 0 stale attempt, -1 already done; a PYTASKQ_WRONGTYPE error (nothing written).
CANCELLED_RUN_SCRIPT = queueing.KEEP_LUA + queueing.TYPE_LUA + """
local bad = pytaskq_need(KEYS[1], 'list') or pytaskq_need(KEYS[3], 'hash') or pytaskq_need(KEYS[4], 'zset')
if bad then return bad end
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 and (ARGV[3] == '1' or redis.call('HGET', KEYS[3], 'status') == 'Cancelled') then return -1 end
local current = redis.call('GET', KEYS[2])
if current and current ~= ARGV[2] then return 0 end
local by = redis.call('GET', KEYS[5])
redis.call('HSET', KEYS[3], 'status', 'Cancelled', 'finished_at', ARGV[5], 'note', ARGV[9], 'error', '',
           'duration_ms', ARGV[10])
if by then redis.call('HSET', KEYS[3], 'cancelled_by', by) end
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[6]))
pytaskq_keep(KEYS[4], ARGV[4], ARGV[5], tonumber(ARGV[7]), ARGV[8])
redis.call('DEL', KEYS[5])
redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
return 1
"""

# A running job's latest progress, only while it's still this worker's and running.
# KEYS: task hash   ARGV: worker id, percent ("" = unchanged), message ("" = unchanged), when
PROGRESS_SCRIPT = """
if redis.call('HGET', KEYS[1], 'status') ~= 'Processing' or redis.call('HGET', KEYS[1], 'worker_id') ~= ARGV[1] then
  return 0
end
if ARGV[2] ~= '' then redis.call('HSET', KEYS[1], 'progress', ARGV[2]) end
if ARGV[3] ~= '' then redis.call('HSET', KEYS[1], 'progress_message', ARGV[3]) end
redis.call('HSET', KEYS[1], 'progress_at', ARGV[4])
return 1
"""

# max_concurrency: fleet-wide leases per task, renewed by the heartbeat, running out if a worker dies.
# Each attempt has its own lease, so a retry can't reuse one that's still running.
# KEYS: the task's lease set, its waiting list   ARGV: lease id, now, lease expiry, limit, key ttl (ms),
# this job's entry in the waiting list ("" = none), "<task id>@promoted".   Returns 1 (go) or 0 (full).
LEASE_SCRIPT = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
-- A finishing job kept its place for this job (the one that waited longest): take it over.
local promoted = redis.call('ZREM', KEYS[1], ARGV[7]) == 1
if not promoted and not redis.call('ZSCORE', KEYS[1], ARGV[1]) and redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[4]) then
  return 0
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
if ARGV[6] ~= '' then redis.call('ZREM', KEYS[2], ARGV[6]) end
return 1
"""

# A max_concurrency job that has to wait: into the delayed set (fallback) and the task's waiting list
# (oldest first), from which a finishing job promotes the next one.
# KEYS: processing list, delayed set, task hash, waiting list
# ARGV: payload as listed, new payload, fallback due time, why it waits, waiting-list entry, waiting since
WAIT_FOR_LEASE_SCRIPT = """
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then return 0 end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
redis.call('ZADD', KEYS[4], ARGV[6], ARGV[5])
redis.call('PEXPIRE', KEYS[4], 604800000)
redis.call('HSET', KEYS[3], 'status', 'Queued', 'payload', ARGV[2], 'waiting_for', ARGV[4])
redis.call('PERSIST', KEYS[3])
return 1
"""

# A max_concurrency lease is given back: move the longest-waiting job onto its queue and keep the
# place for it. Entries are "queue list\nwake list\ntask id\npayload"; ones whose job already left the
# delayed set are skipped.
# KEYS: lease set, waiting list, delayed set   ARGV: lease id, now, seconds the place is kept
RELEASE_SCRIPT = queueing.WAKE_LUA + """
redis.call('ZREM', KEYS[1], ARGV[1])
for i = 1, 20 do
    local first = redis.call('ZRANGE', KEYS[2], 0, 0)[1]
    if not first then return 0 end
    redis.call('ZREM', KEYS[2], first)
    local a = string.find(first, '\\n', 1, true)
    local b = a and string.find(first, '\\n', a + 1, true)
    local c = b and string.find(first, '\\n', b + 1, true)
    if c then
        local payload = string.sub(first, c + 1)
        if redis.call('ZREM', KEYS[3], payload) == 1 then
            redis.call('ZADD', KEYS[1], tonumber(ARGV[2]) + tonumber(ARGV[3]),
                       string.sub(first, b + 1, c - 1) .. '@promoted')
            redis.call('RPUSH', string.sub(first, 1, a - 1), payload)
            pytaskq_wake(string.sub(first, a + 1, b - 1), 100)
            return 1
        end
    end
end
return 0
"""

# rate_limit: starts at least one gap apart across the fleet. A job reserves a turn one gap after the
# latest one and waits until then; at start, its turn must be due and the previous start a gap ago.
# A due turn (at most max(gap, 1 s) old) blocked by a late previous start keeps its place; older turns
# are stale and go to the end of the line, so they can't burst. Turns are per job, so a cancelled job
# gives its turn back.
# The time is Redis's own (TIME), read inside the script: a time computed by the worker before sending
# could be stale or skewed, letting two workers start closer than the gap.
# KEYS: the task's turns, its last start
# ARGV: now ("" = Redis's clock; a fixed time for tests), seconds between starts, task id, this job's turn ("")
#       [, "confirm": the job got its "go" earlier and is really starting now (it waited for a thread)]
# Returns "go" (start now; recorded as the latest start) or the time of its (new) turn. With "confirm", a job
# whose own "go" is still the latest start moves that start to now, so the next turn counts from its real start.
RATE_SCRIPT = """
local now, gap = tonumber(ARGV[1]), tonumber(ARGV[2])
if not now then
    local t = redis.call('TIME')
    now = tonumber(t[1]) + tonumber(t[2]) / 1000000
end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - 2 * gap - 1)
local last_start = tonumber(redis.call('GET', KEYS[2]) or '0')
local mine = tonumber(ARGV[4] ~= '' and ARGV[4] or redis.call('ZSCORE', KEYS[1], ARGV[3]) or '')
local ttl = math.ceil(3 * gap * 1000) + 60000
local early = math.min(0.05, gap / 2)       -- a reserved turn this close counts as due (the gap still holds)
if mine and mine > now + early then
    if redis.call('ZSCORE', KEYS[1], ARGV[3]) then return string.format('%.6f', mine) end   -- not yet
    mine = nil                                              -- its turn is gone: a new one below
end
local function start()
    redis.call('SET', KEYS[2], string.format('%.6f', now), 'PX', ttl)
    redis.call('ZADD', KEYS[1], now, ARGV[3])
    redis.call('PEXPIRE', KEYS[1], ttl)
    return 'go'
end
if ARGV[5] == 'confirm' then
    local own = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[3]) or '')
    if own and math.abs(own - last_start) < 0.00001 then return start() end
end
if mine and now >= last_start + gap * 0.999 then return start() end
if mine and mine >= now - math.max(gap, 1.0) then
    local at = last_start + gap
    redis.call('ZADD', KEYS[1], at, ARGV[3])
    redis.call('PEXPIRE', KEYS[1], math.ceil((at - now) * 1000) + ttl)
    return string.format('%.6f', at)
end
redis.call('ZREM', KEYS[1], ARGV[3])
local slot = math.max(now, last_start + gap)
local latest = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if latest[2] then slot = math.max(slot, tonumber(latest[2]) + gap) end
if slot <= now and now >= last_start + gap * 0.999 then return start() end
redis.call('ZADD', KEYS[1], slot, ARGV[3])
redis.call('PEXPIRE', KEYS[1], math.ceil((slot - now) * 1000) + ttl)
return string.format('%.6f', slot)
"""

# Move due jobs from the delayed set onto their queues while each queue has room (QUEUE_CAPACITY).
# KEYS: delayed set, queue-name set, each queue's 3 lists, each queue's wake list, then per job: target list,
#       task hash
# ARGV: capacity (0 = none), number of queues, the queue names, then per job: payload, queue index (1-based)
# Returns {moved, skipped because the queue was full, the indexes (1-based) of the queues that were full}
PROMOTE_SCRIPT = queueing.WAKE_LUA + """
local cap = tonumber(ARGV[1])
local nq = tonumber(ARGV[2])
local counts = {}
local used = {}
local moved, full = 0, 0
local fullq, fulllist = {}, {}
local base = 2 + nq * 4
local first = 2 + nq
local n = (#ARGV - first) / 2
for i = 1, n do
    local raw = ARGV[first - 1 + 2 * i]
    local qi = tonumber(ARGV[first + 2 * i])
    local room = true
    if cap > 0 then
        if counts[qi] == nil then
            local k = 2 + (qi - 1) * 3
            counts[qi] = redis.call('LLEN', KEYS[k + 1]) + redis.call('LLEN', KEYS[k + 2])
                         + redis.call('LLEN', KEYS[k + 3])
        end
        room = counts[qi] < cap
    end
    if not room then
        full = full + 1
        if not fullq[qi] then
            fullq[qi] = true
            table.insert(fulllist, qi)
        end
    elseif redis.call('ZREM', KEYS[1], raw) == 1 then
        redis.call('LPUSH', KEYS[base + 2 * i - 1], raw)
        if cap > 0 then counts[qi] = counts[qi] + 1 end
        local hash = KEYS[base + 2 * i]
        local status = redis.call('HGET', hash, 'status')
        if status == 'Scheduled' or status == 'RetryScheduled' then
            redis.call('HSET', hash, 'status', 'Queued')
        end
        used[qi] = true
        moved = moved + 1
    end
end
for qi, _ in pairs(used) do
    redis.call('SADD', KEYS[2], ARGV[2 + qi])
    pytaskq_wake(KEYS[2 + nq * 3 + qi], 100)
end
if moved > 0 then redis.call('PUBLISH', '__PYTASKQ__events:global', 'update') end
return {moved, full, fulllist}
"""

# An invalid job found in a queue or the delayed set becomes a dead letter instead of being dropped.
# KEYS: source (list or delayed set), dlq, task hash, history, failed counter, finished-jobs set
# ARGV: raw text, "list" | "zset", dead-letter entry, task id, error, time, result TTL, history limit, dlq limit,
#       jobs kept, task-key prefix
INVALID_SCRIPT = queueing.KEEP_LUA + """
local removed
if ARGV[2] == 'zset' then
    removed = redis.call('ZREM', KEYS[1], ARGV[1])
else
    removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
end
if removed == 0 then return 0 end
redis.call('LPUSH', KEYS[2], ARGV[3])
local limit = tonumber(ARGV[9])
if limit > 0 and redis.call('LLEN', KEYS[2]) > limit then redis.call('LTRIM', KEYS[2], 0, limit - 1) end
redis.call('HSET', KEYS[3], 'task_id', ARGV[4], 'task_name', '(invalid job)', 'status', 'DeadLetter',
           'queue', 'default', 'error', ARGV[5], 'created_at', ARGV[6], 'finished_at', ARGV[6])
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[7]))
pytaskq_keep(KEYS[6], ARGV[4], ARGV[6], tonumber(ARGV[10]), ARGV[11])
redis.call('LPUSH', KEYS[4], ARGV[4])
redis.call('LTRIM', KEYS[4], 0, tonumber(ARGV[8]) - 1)
redis.call('INCR', KEYS[5])
redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
return 1
"""

# A job really starts: record it. (PERSIST first, so this works when Redis is full.)
# KEYS: task hash [, cancel request [, fence]]   ARGV: field, value, … [, this attempt's fence token: with KEYS[3]]
# Returns 1 started, 0 cancelled, -1 stale: the job was handed to another worker meanwhile (its fence
# moved on), so this attempt must not run or touch the record.
STARTED_SCRIPT = """
local unpack = unpack or table.unpack
local last = #ARGV
if KEYS[3] then
    local current = redis.call('GET', KEYS[3])
    if current and current ~= ARGV[#ARGV] then return -1 end
    last = #ARGV - 1
end
-- Cancelled while it waited: don't start it.
if KEYS[2] and redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
redis.call('PERSIST', KEYS[1])
redis.call('HDEL', KEYS[1], 'progress', 'progress_message', 'progress_at', 'waiting_for')   -- a retry starts from zero
redis.call('HSET', KEYS[1], unpack(ARGV, 1, last))
redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
return 1
"""

# "This worker is alive" (works when Redis is full).
# KEYS: active_workers, worker hash, workers set, queue-name set
# ARGV: worker id, alive until, hash TTL, number of queue names, queue names…, hash fields… (with boot_id)
# Returns 1, or 0 when the name now belongs to another process (a different boot_id): nothing written.
HEARTBEAT_SCRIPT = OOM_PROBE + """
local unpack = unpack or table.unpack
-- Another live process took this name (after this one was paused past its heartbeat): stop.
local nq0 = tonumber(ARGV[4])
for i = 5 + nq0, #ARGV - 1, 2 do
    if ARGV[i] == 'boot_id' then
        local owner = redis.call('HGET', KEYS[2], 'boot_id')
        if owner and owner ~= '' and owner ~= ARGV[i + 1] then return 0 end
    end
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[1])
local nq = tonumber(ARGV[4])
if nq > 0 then redis.call('SADD', KEYS[4], unpack(ARGV, 5, 4 + nq)) end
if #ARGV > 4 + nq then redis.call('HSET', KEYS[2], unpack(ARGV, 5 + nq)) end
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))
return 1
"""

# Forget a dead worker, unless it sent a heartbeat meanwhile.
# KEYS: active_workers, worker hash, workers set   ARGV: worker id, now
FORGET_WORKER_SCRIPT = """
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) > tonumber(ARGV[2]) then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
redis.call('SREM', KEYS[3], ARGV[1])
return 1
"""


fill_scripts(globals())     # put the key prefix into the Lua scripts above
