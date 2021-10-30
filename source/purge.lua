-- --This script is not redis cluster compatible as it uses dynamic keys at runtime which are renamed apart from KEYS passed in.
local pendingSetsKey = KEYS[1]
local metaLastSetTimeKey = KEYS[2]
local metaCountKey = KEYS[3]
local metaBytesKey = KEYS[4]
local tokenToSetnameKey = KEYS[5]

local pendingSortedSetsTimeoutInSeconds = tonumber(ARGV[1])
local lastUpsertElapsedTimeInSeconds = tonumber(ARGV[2])
local maxSortedSetsToRetrive = tonumber(ARGV[3])
local maximumCountThreshold = tonumber(ARGV[4])
local maximumBytesThreshold = tonumber(ARGV[5])
local purgeKeyAppend = ARGV[6]
local setnameToTokenKeyAppend = KEYS[7]

local tempTime = redis.call("TIME")
local currentTimestampInSeconds = tonumber(tempTime[1])
local purgableSets = {}
local returnSets = {}

function table.merge(t1, t2)
    for k,v in ipairs(t2) do
       table.insert(t1, v)
    end 
    return t1
 end

--Query Pending sortedsets
local adjustedExpiryTime = (currentTimestampInSeconds - pendingSortedSetsTimeoutInSeconds)
purgableSets = redis.call('ZRANGEBYSCORE',pendingSetsKey,'-inf',adjustedExpiryTime,'LIMIT',0,maxSortedSetsToRetrive)
redis.call('ZREM',pendingSetsKey,unpack(purgableSets))

--Query Elapsed sortedsets
if(lastUpsertElapsedTimeInSeconds ~= nil and #purgableSets < maxSortedSetsToRetrive) then
    local adjustedElapsedTime = (currentTimestampInSeconds - lastUpsertElapsedTimeInSeconds)
    local sets = redis.call('ZRANGEBYSCORE',metaLastSetTimeKey,'-inf',adjustedExpiryTime,'LIMIT',0,(maxSortedSetsToRetrive-#purgableSets))
    redis.call('ZREM',metaLastSetTimeKey,unpack(sets))
    purgableSets = table.merge(purgableSets,sets)
end

--Query Count sortedsets
if(maximumCountThreshold ~= nil and #purgableSets < maxSortedSetsToRetrive) then
    local sets = redis.call('ZRANGEBYSCORE',metaCountKey,'-inf',maximumCountThreshold,'LIMIT',0,(maxSortedSetsToRetrive-#purgableSets))
    redis.call('ZREM',metaCountKey,unpack(sets))
    purgableSets = table.merge(purgableSets,sets)
end

--Query Bytes sortedsets
if(maximumBytesThreshold ~= nil and #purgableSets < maxSortedSetsToRetrive) then
    local sets = redis.call('ZRANGEBYSCORE',metaBytesKey,'-inf',maximumBytesThreshold,'LIMIT',0,(maxSortedSetsToRetrive-#purgableSets))
    redis.call('ZREM',metaBytesKey,unpack(sets))
    purgableSets = table.merge(purgableSets,sets)
end

--Dump sortedsets
for index = 1, #purgableSets do
    local nameOrToken = purgableSets[index]
    local setName = redis.call('HGET',tokenToSetnameKey,nameOrToken) or nameOrToken
    local setData = redis.call('ZRANGEBYSCORE',nameOrToken,'-inf','+inf','WITHSCORES')
    local newToken = setName .. purgeKeyAppend
    if (setName ~= nameOrToken) then --This means nameOrToken is token
        newToken = nameOrToken .. purgeKeyAppend
        redis.call('HDEL',(setName .. setnameToTokenKeyAppend),nameOrToken)
    end
    redis.call('ZADD',pendingSetsKey,currentTimestampInSeconds,newToken);
    redis.call('HSET',tokenToSetnameKey,newToken,setName);
    redis.call('HSET',(setName .. setnameToTokenKeyAppend),setName,newToken);
    redis.call("RENAMENX",nameOrToken,newToken)
    table.insert(returnSets,{newToken,setData})
end

return returnSets

-- return acquiredPartitions
-- docker run with -v ${PWD}:\"/var/lib/mysql\"
-- cd /var/lib/mysql/lua-scripts
-- redis-cli --eval purge-acquire.lua space-rac space-pen , 1 0 1 10 token1 space - pur
-- Stringyfied Response [["[\"GapTag-0-pur\",[\"2b8pjZTY>G\"]]",["{\"p\":\"One\",\"u\":\"1629634010341-2b8pjZTY>G-0\"}","1","{\"p\":\"Two\",\"u\":\"1629634010341-2b8pjZTY>G-1\"}","2"]],["[\"GapTag-10-pur\",[\"2b8pjZTY>G\"]]",["{\"p\":\"Ten\",\"u\":\"1629634010341-2b8pjZTY>G-2\"}","0"]],["[\"GapTag-20-pur\",[\"2b8pjZTY>G\"]]",["{\"p\":\"Twenty\",\"u\":\"1629634010341-2b8pjZTY>G-3\"}","0"]],["[\"SerialTag-0-pur\",[\"2b8pjZTY>G\"]]",["{\"p\":\"One\",\"u\":\"1629634010341-2b8pjZTY>G-4\"}","1","{\"p\":\"Two\",\"u\":\"1629634010341-2b8pjZTY>G-5\"}","2","{\"p\":\"Three\",\"u\":\"1629634010341-2b8pjZTY>G-6\"}","3","{\"p\":\"Four\",\"u\":\"1629634010341-2b8pjZTY>G-7\"}","4"]]]