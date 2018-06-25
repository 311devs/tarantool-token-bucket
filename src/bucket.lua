-- inspired by https://medium.com/smyte/rate-limiter-df3408325846
local fiber = require('fiber')

local Bucket = {}
Bucket.__index = Bucket

function Bucket:new(max_amount, refill_time, refill_amount)
    local bucket = {}
    setmetatable(bucket, Bucket)
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
    local row = box.space._token_bucket:get{key}
    if row ~= nil then
        return row[2], row[3]
    end
    return self.max_amount, now
end

function Bucket:_update_state(key, value, last_update)
    box.space._token_bucket:put{key, value, last_update}
end

function Bucket:get(key)
    local now = self:_now()
    local value, last_update = self:_get_state(key, now)
    local refill_count = self:_refill_count(now, last_update)
    return math.min(self.max_amount, value + refill_count * self.refill_amount)
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

return Bucket