-- inspired by https://medium.com/smyte/rate-limiter-df3408325846
local fiber = require('fiber')

local Bucket = {}
Bucket.__index = Bucket

function Bucket:new(kind, max_amount, refill_time, refill_amount)
    local bucket = {}
    setmetatable(bucket, Bucket)
    bucket.kind = kind
    bucket.max_amount = max_amount
    bucket.refill_time = refill_time
    bucket.refill_amount = refill_amount

    return bucket
end

function Bucket:_now()
    return math.floor(fiber.time())
end

function Bucket:_refill_count(now, last_update)
    return math.floor((now - last_update) / self.refill_time)
end

function Bucket:_get_state(key, now)
    local row = box.space._token_bucket:get{key, self.kind}
    if row ~= nil then
        return row[3], row[4]
    end
    return self.max_amount, now
end

function Bucket:_update_state(key, value, last_update)
    box.space._token_bucket:put{key, self.kind, value, last_update}
end

function Bucket:get(key, is_row)
    local now = self:_now()
    local value, last_update
    if is_row then
        value, last_update = key[3], key[4]
    else
        value, last_update = self:_get_state(key, now)
    end
    local refill_count = self:_refill_count(now, last_update)
    return math.min(self.max_amount, value + refill_count * self.refill_amount)
end

function Bucket:reset(key)
    box.space._token_bucket:delete{key, self.kind}
end

function Bucket:reduce(key, tokens)
    local now = self:_now()
    local value, last_update = self:_get_state(key, now)
    local refill_count = self:_refill_count(now, last_update)

    value = value + refill_count * self.refill_amount
    last_update = last_update + refill_count * self.refill_time

    if value >= self.max_amount then
        value = self.max_amount
        last_update = now
    end
    if tokens > value then
        self:_update_state(key, value, last_update)
        return false
    end

    value = value - tokens
    self:_update_state(key, value, last_update)
    return true
end

function clear(tb_params, chunk_size)
    local space = box.space._token_bucket
    local tbs = {}
    for k, v in pairs(tb_params) do
        tbs[k] = Bucket:new(k, v['max_amount'], v['refill_time'], v['refill_amount'])
    end

    local deleted = 0
    local visited = 0
    for _, row in space:pairs() do
        local key, kind = row[1], row[2]
        local tb = tbs[kind]
        visited = visited + 1
        if tb and tb:get(row, true) == tb['max_amount'] then
            space:delete{key, kind}
            deleted = deleted + 1
        end
        if visited == chunk_size then
            visited = 0
            fiber.yield()
        end
    end
    return deleted
end

return {
    Bucket = Bucket,
    clear = clear,
}