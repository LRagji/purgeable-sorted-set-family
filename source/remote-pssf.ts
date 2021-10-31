import { IPurgeableSortedSetFamily, ISortedStringData } from ".";
import { IBulkResponse } from "./i-bulk-response";
import { IError } from "./i-error";
import { IRedisClient } from "./i-redis-client";
import Crypto from "crypto";
import path from "path";
// @ts-ignore
import SortedSet from "redis-sorted-set";

const minimumScore = -9007199254740992n;
const maximumScore = 9007199254740992n;

export class RemotePSSF implements IPurgeableSortedSetFamily<ISortedStringData> {
    private redisClientResolver: (operation: Operation) => Promise<IRedisClient>;
    private activityKey: string;
    private keyPrefix: string;
    private countKey: string;
    private bytesKey: string;
    private purgeKeyAppend: string;
    private pendingSetKey: string;
    private setnamesToTokensKeyAppend: string;
    private tokenToSetName: string;
    private redisCommandZADD = 'zadd';
    private redisCommandZINCRBY = 'zincrby';
    private redisCommandMEMORY = 'memory';
    private redisCommandMEMORYOptions = 'usage';
    private redisCommandLRANGE = 'lrange';
    private redisCommandZRANGEBYSCORE = 'zrangebyscore';
    private redisCommandZRANGEBYSCOREOptionWITHSCORES = 'withscores';


    constructor(redisClientResolver: (operation: Operation) => Promise<IRedisClient>,
        purgeKeyAppend: string = "-Purged", setnamesToTokensKeyAppend = "SNTS", tokenToSetName = "TSN",
        pendingSetKey: string = "Pending",
        activityKey: string = "Activity", countKey: string = "Stats", bytesKey: string = "Bytes") {

        //TODO Reimplement keys validations
        //purgeKeyAppend cannot be empty string or it will not able to distinguish between normal set and purged set as names will remain same.

        if (activityKey.endsWith(purgeKeyAppend) || countKey.endsWith(purgeKeyAppend) || bytesKey.endsWith(purgeKeyAppend) || tokenToSetName.endsWith(purgeKeyAppend) || setnamesToTokensKeyAppend.endsWith(purgeKeyAppend) || pendingSetKey.endsWith(purgeKeyAppend)) {
            throw new Error(`Reserved keys "${activityKey},${countKey},${bytesKey},${tokenToSetName},${setnamesToTokensKeyAppend}" cannot end with "purgeKeyAppend"(${purgeKeyAppend}).`);
        }
        if (activityKey === countKey || activityKey === bytesKey || countKey === bytesKey || tokenToSetName === setnamesToTokensKeyAppend
            || activityKey === setnamesToTokensKeyAppend || bytesKey === setnamesToTokensKeyAppend || countKey === setnamesToTokensKeyAppend
            || activityKey === tokenToSetName || bytesKey === tokenToSetName || countKey === tokenToSetName) {
            throw new Error(`Reserved keys "${activityKey},${countKey},${bytesKey},${tokenToSetName},${setnamesToTokensKeyAppend}" cannot be same within.`);
        }

        this.redisClientResolver = redisClientResolver;
        this.keyPrefix = this.settingsHash({ "activityKey": activityKey, "countKey": countKey, "bytesKey": bytesKey, "purgeKeyAppend": purgeKeyAppend, "setnamesToTokensKeyAppend": setnamesToTokensKeyAppend, "tokenToSetName": tokenToSetName });
        this.activityKey = activityKey;
        this.countKey = countKey;
        this.bytesKey = bytesKey;
        this.purgeKeyAppend = purgeKeyAppend;
        this.setnamesToTokensKeyAppend = setnamesToTokensKeyAppend;
        this.tokenToSetName = tokenToSetName;
        this.pendingSetKey = pendingSetKey;
    }

    private settingsHash(settings: object) {
        return Crypto.createHash("sha256").update(JSON.stringify(settings), "binary").digest("hex");
    }

    async upsert(data: ISortedStringData[]): Promise<IBulkResponse<ISortedStringData[], IError<ISortedStringData>[]>> {
        const returnObject = { succeeded: new Array<ISortedStringData>(), failed: new Array<IError<ISortedStringData>>() };
        const zaddCommands = new Map<string, string[]>();
        data.forEach(ss => {
            if (ss.score >= maximumScore || ss.score <= minimumScore) {
                returnObject.failed.push({ data: ss, error: new Error(`Score(${ss.score}) for set named "${ss.setName}" is not within range of ${minimumScore} to ${maximumScore}.`) });
            }
            else if (ss.setName.endsWith(this.purgeKeyAppend) === true) {
                returnObject.failed.push({ data: ss, error: new Error(`Setname "${ss.setName}" cannot end with system reserved key "${this.purgeKeyAppend}".`) });
            }
            else if (ss.setName === this.activityKey || ss.setName === this.countKey || ss.setName === this.bytesKey || ss.setName === this.tokenToSetName) {
                returnObject.failed.push({ data: ss, error: new Error(`Setname "${ss.setName}" cannot match any of the system reserved keys "${this.activityKey},${this.countKey},${this.bytesKey},${this.tokenToSetName}".`) });
            }
            else if (ss.setName.endsWith(this.setnamesToTokensKeyAppend) === true) {
                returnObject.failed.push({ data: ss, error: new Error(`Setname "${ss.setName}" cannot end with system reserved key "${this.setnamesToTokensKeyAppend}".`) });
            }
            else {
                const commands = zaddCommands.get(ss.setName) || new Array<string>();
                commands.push(ss.score.toString(), ss.payload);
                zaddCommands.set(ss.setName, commands);
                returnObject.succeeded.push(ss);
            }
        });
        const client = await this.redisClientResolver(Operation.ReadNWrite);
        const token = "upsert" + Date.now();
        try {
            const setsPipeline = new Array<Array<string>>();
            const setsBytesPipeline = new Array<Array<string>>();
            let setsBytesUpdatesPipeline = new Array<Array<string>>();
            zaddCommands.forEach((values, sortedSetName) => {
                setsPipeline.push([this.redisCommandZADD, (this.keyPrefix + sortedSetName), ...values],//Main Sorted Set
                    [this.redisCommandZADD, (this.keyPrefix + this.activityKey), (Date.now() / 1000).toFixed(0), sortedSetName], //Sets Activity Time
                    [this.redisCommandZINCRBY, (this.keyPrefix + this.countKey), (values.length / 2).toFixed(0), sortedSetName]); //Sets Count
                setsBytesPipeline.push([this.redisCommandMEMORY, this.redisCommandMEMORYOptions, (this.keyPrefix + sortedSetName)]);//Get Bytes Query
                setsBytesUpdatesPipeline.push([this.redisCommandZADD, (this.keyPrefix + this.bytesKey), "0", sortedSetName]);//Sets total Bytes
            });
            await client.acquire(token);
            await client.pipeline(setsPipeline);
            const bytesResults: string[] = await client.pipeline(setsBytesPipeline);
            setsBytesUpdatesPipeline = setsBytesUpdatesPipeline.map((e, idx) => { e[2] = bytesResults[idx]; return e; });
            await client.pipeline(setsBytesUpdatesPipeline);
        }
        finally {
            await client.release(token);
        }
        return Promise.resolve(returnObject);
    }

    async scoreRangeQuery(setName: string, scoreStart: bigint, scoreEnd: bigint): Promise<IError<ISortedStringData[]>> {
        const returnObject: IError<ISortedStringData[]> = { data: new Array<ISortedStringData>(), error: undefined };
        if (scoreStart > scoreEnd) {
            returnObject.error = new Error(`Invalid range start(${scoreStart}) cannot be greator than end(${scoreEnd}).`)
        }
        else {
            const client = await this.redisClientResolver(Operation.Read);
            const token = "scoreRangeQuery" + Date.now();
            try {
                client.acquire(token)
                const setNamesToQuery = await client.run([this.redisCommandLRANGE, (this.keyPrefix + setName + this.setnamesToTokensKeyAppend), "0", "-1"]) as string[] || [];
                setNamesToQuery.push(setName);
                const query = setNamesToQuery.map(setName => [this.redisCommandZRANGEBYSCORE, (this.keyPrefix + setName), scoreStart.toString(), scoreEnd.toString(), this.redisCommandZRANGEBYSCOREOptionWITHSCORES]);
                const results = await client.pipeline(query) as Array<Array<string>>;
                if (query.length === 1) {
                    const singularSortedSetResult = results[0];
                    for (let index = 0; index < singularSortedSetResult.length; index += 2) {
                        returnObject.data.push({ score: BigInt(singularSortedSetResult[index + 1]), setName: setName, payload: singularSortedSetResult[index] });
                    };
                }
                else {
                    const unionSet = results.reduce((acc, r) => {
                        for (let index = 0; index < r.length; index += 2) {
                            acc.add(r[index], BigInt(r[index + 1]));
                        };
                        return acc;
                    }, new SortedSet());
                    const finalResults = unionSet.rangeByScore(scoreStart, scoreEnd, { withScores: true });
                    returnObject.data = finalResults.map((e: Array<Array<any>>) => ({ score: e[1], setName: setName, payload: e[0] }));
                }
            }
            finally {
                await client.release(token);
            }
        }
        return returnObject;
    }

    async purgeBegin(lastUpsertElapsedTimeInSeconds: number | null, maximumCountThreshold: number | null, maximumBytesThreshold: bigint | null, pendingSortedSetsTimeoutInSeconds: number = 3600, maxSortedSetsToRetrive: number = 10): Promise<IError<Map<string, ISortedStringData[]>>> {
        const returnObject: IError<Map<string, Array<ISortedStringData>>> = { data: new Map<string, Array<ISortedStringData>>(), error: undefined };
        const client = await this.redisClientResolver(Operation.Script);
        try {
            const keys: string[] = [this.pendingSetKey, this.activityKey, this.countKey, this.bytesKey, this.tokenToSetName];
            const args: string[] = [pendingSortedSetsTimeoutInSeconds.toFixed(0),
            lastUpsertElapsedTimeInSeconds == null ? 'nil' : lastUpsertElapsedTimeInSeconds.toFixed(0),
            maxSortedSetsToRetrive.toFixed(0),
            maximumCountThreshold == null ? 'nil' : maximumCountThreshold.toFixed(0),
            maximumBytesThreshold == null ? 'nil' : maximumBytesThreshold.toString(),
            this.purgeKeyAppend, this.setnamesToTokensKeyAppend, this.keyPrefix];
            const results = await client.script(path.join(__dirname, 'purge-begin.lua'), keys, args) as Array<Array<string | Array<string>>>;
            results.forEach(m => {
                const token = m[0] as string;
                const setName = m[1] as string;
                const newData = m[2] as Array<string>;
                // console.log(token)
                // console.log(setName)
                // console.log(newData)
                const existingData = returnObject.data.get(token) || new Array<ISortedStringData>();
                for (let index = 0; index < newData.length; index += 2) {
                    const member = newData[index];
                    const score = newData[index + 1];
                    // console.log(member)
                    // console.log(BigInt(score))
                    existingData.push({ score: BigInt(score), setName: setName, payload: member });
                }
                returnObject.data.set(token, existingData);
            });
        }
        finally {
            await client.release();
        }
        return returnObject;
    }

    async purgeEnd(tokens: string[]): Promise<IBulkResponse<string[], IError<string>[]>> {
        const returnObject = { succeeded: new Array<string>(), failed: new Array<IError<string>>() };
        const client = await this.redisClientResolver(Operation.Script);
        try {
            for (let index = 0; index < tokens.length; index++) {
                const token = tokens[index];
                const keys: string[] = [this.pendingSetKey, this.tokenToSetName];
                const args: string[] = [this.setnamesToTokensKeyAppend, this.keyPrefix, token];
                const result = await client.script(path.join(__dirname, 'purge-end.lua'), keys, args) as number;
                if (result === 1) {
                    returnObject.succeeded.push(token);
                }
                else {
                    const err = new Error(`Token "${token}" doesnot exists.`);
                    err.stack = "Redis return code " + result;
                    returnObject.failed.push({ error: err, data: token });
                }
            }
        }
        finally {
            await client.release();
        }
        return returnObject;
    }
}

export enum Operation {
    Read,
    Write,
    ReadNWrite,
    Script
}