--This script is not redis cluster compatible as it uses dynamic keys at runtime which are renamed apart from KEYS passed in.
local keyPrepend = ARGV[2]
local pendingSetsKey = keyPrepend .. KEYS[1]
local tokenToSetnameKey = keyPrepend .. KEYS[2]

local setnameToTokenKeyAppend = ARGV[1]
local token = ARGV[3]

local deleteCount = redis.call('ZREM',pendingSetsKey,token)
if(deleteCount == 1) then
    local setname = redis.call('HGET',tokenToSetnameKey,token)
    redis.call('HDEL',tokenToSetnameKey,token)
    redis.call('LREM',(keyPrepend .. setname .. setnameToTokenKeyAppend),0,token)
    redis.call('DEL',(keyPrepend .. token))
    return 1;
else
    return 0;
end