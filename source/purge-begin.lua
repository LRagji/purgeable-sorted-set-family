-- --This script is not redis cluster compatible as it uses dynamic keys at runtime which are renamed apart from KEYS passed in.
local keyPrepend = ARGV[8]
local pendingSetsKey = keyPrepend .. KEYS[1]
local metaLastSetTimeKey = keyPrepend .. KEYS[2]
local metaCountKey = keyPrepend .. KEYS[3]
local metaBytesKey = keyPrepend .. KEYS[4]
local tokenToSetnameKey = keyPrepend .. KEYS[5]

local pendingSortedSetsTimeoutInSeconds = tonumber(ARGV[1])
local lastUpsertElapsedTimeInSeconds = tonumber(ARGV[2])
local maxSortedSetsToRetrive = tonumber(ARGV[3])
local maximumCountThreshold = tonumber(ARGV[4])
local maximumBytesThreshold = tonumber(ARGV[5])
local purgeKeyAppend = ARGV[6]
local setnameToTokenKeyAppend = ARGV[7]

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

--local logs = {}

--Query Pending sortedsets
local adjustedExpiryTime = (currentTimestampInSeconds - pendingSortedSetsTimeoutInSeconds)
purgableSets = redis.call('ZRANGEBYSCORE',pendingSetsKey,'-inf',adjustedExpiryTime,'LIMIT',0,maxSortedSetsToRetrive)
if (#purgableSets > 0) then
    redis.call('ZREM',pendingSetsKey,unpack(purgableSets))
end
--table.insert(logs,'Found '.. #purgableSets .. ' pending entries ' .. adjustedExpiryTime)

--Query Elapsed sortedsets
if(lastUpsertElapsedTimeInSeconds ~= nil and #purgableSets < maxSortedSetsToRetrive) then
    local adjustedElapsedTime = (currentTimestampInSeconds - lastUpsertElapsedTimeInSeconds)
    local sets = redis.call('ZRANGEBYSCORE',metaLastSetTimeKey,'-inf',adjustedElapsedTime,'LIMIT',0,(maxSortedSetsToRetrive-#purgableSets))
    if (#sets > 0) then
        redis.call('ZREM',metaLastSetTimeKey,unpack(sets))
    end
    purgableSets = table.merge(purgableSets,sets)
    --table.insert(logs,'Found '.. #purgableSets .. ' elapsed entries ' .. adjustedElapsedTime)
end

--Query Count sortedsets
if(maximumCountThreshold ~= nil and #purgableSets < maxSortedSetsToRetrive) then
    local sets = redis.call('ZRANGEBYSCORE',metaCountKey,maximumCountThreshold,'+inf','LIMIT',0,(maxSortedSetsToRetrive-#purgableSets))
    if (#sets > 0) then
        redis.call('ZREM',metaCountKey,unpack(sets))
    end
    purgableSets = table.merge(purgableSets,sets)
    --table.insert(logs,'Found '.. #purgableSets .. ' count entries '.. maximumCountThreshold)
end


--Query Bytes sortedsets
if(maximumBytesThreshold ~= nil and #purgableSets < maxSortedSetsToRetrive) then
    local sets = redis.call('ZRANGEBYSCORE',metaBytesKey,maximumBytesThreshold,'+inf','LIMIT',0,(maxSortedSetsToRetrive-#purgableSets))
    if (#sets > 0) then
        redis.call('ZREM',metaBytesKey,unpack(sets))
    end
    purgableSets = table.merge(purgableSets,sets)
    --table.insert(logs,'Found '.. #purgableSets .. ' bytes entries ' .. metaBytesKey)
end


--Dump sortedsets
for index = 1, #purgableSets do
    local nameOrToken = purgableSets[index]
    --table.insert(logs,'Dumping started '.. nameOrToken )
    local setName = redis.call('HGET',tokenToSetnameKey,nameOrToken) or nameOrToken
    --table.insert(logs,nameOrToken .. ': Set name '.. setName )
    local setData = redis.call('ZRANGEBYSCORE',(keyPrepend .. nameOrToken),'-inf','+inf','WITHSCORES')
    local newToken = setName .. purgeKeyAppend
    if (setName ~= nameOrToken) then --This means nameOrToken is token
        newToken = nameOrToken .. purgeKeyAppend
        redis.call('LREM',(keyPrepend .. setName .. setnameToTokenKeyAppend),0,nameOrToken)
    end
    --table.insert(logs,nameOrToken .. ': Found entries '.. #setData )
    redis.call('ZADD',pendingSetsKey,currentTimestampInSeconds,newToken);
    redis.call('HSET',tokenToSetnameKey,newToken,setName);
    redis.call('RPUSH',(keyPrepend .. setName .. setnameToTokenKeyAppend),newToken);
    redis.call("RENAMENX",(keyPrepend .. nameOrToken),(keyPrepend .. newToken))
    table.insert(returnSets,{newToken, setName, setData})
    --table.insert(logs,'Dumping finished '.. nameOrToken )
end
--table.insert(returnSets,logs)
return returnSets