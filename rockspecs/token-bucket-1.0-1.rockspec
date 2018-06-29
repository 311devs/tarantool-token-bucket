package = 'token-bucket'
version = '1.0-1'


source = {
    url = 'git://github.com/311devs/tarantool-token-bucket.git'
}

dependencies = {
    'lua >= 5.1'
}

build = {
    type = 'builtin',
    modules = {
        ['token-bucket.bucket'] = 'src/bucket.lua',
        ['token-bucket'] = 'src/init.lua'
    }
}
