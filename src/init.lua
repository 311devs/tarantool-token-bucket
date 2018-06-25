local module = {
    Bucket = require('token-bucket.bucket')
}

-- create infrustructure
function module.start()
    local _token_bucket = box.space._token_bucket
    if _token_bucket == nil then
        _token_bucket = box.schema.create_space('_token_bucket', {
            -- temporary = true,
            format = {
                {name = 'key', type = 'string'},
                {name = 'value', type = 'number'},
                {name = 'last_update', type = 'number'},
            }
        })
        _token_bucket:create_index('pk', {
            type = 'tree',
            parts = {1, 'string'},
            unique = true
        })
    end
end

return module
