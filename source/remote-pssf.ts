import { IPurgeableSortedSetFamily, ISortedStringData } from ".";
import { IBulkResponse } from "./i-bulk-response";
import { IError } from "./i-error";
import { IRedisClient } from "./i-redis-client";
import Crypto from "crypto";
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
    private setnamesToTokensKeyAppend: string;
    private tokenToSetName: string;
    private redisCommandZADD = 'zadd';
    private redisCommandZINCRBY = 'zincrby';
    private redisCommandMEMORY = 'memory';
    private redisCommandMEMORYOptions = 'usage';
    private redisCommandHGET = 'hget';
    private redisCommandZRANGEBYSCORE = 'zrangebyscore';
    private redisCommandZRANGEBYSCOREOptionWITHSCORES = 'withscores';


    constructor(redisClientResolver: (operation: Operation) => Promise<IRedisClient>,
        purgeKeyAppend: string = "-Purged", setnamesToTokensKeyAppend = "SNTS", tokenToSetName = "TSN",
        activityKey: string = "Activity", countKey: string = "Stats", bytesKey: string = "Bytes") {

        if (activityKey.endsWith(purgeKeyAppend) || countKey.endsWith(purgeKeyAppend) || bytesKey.endsWith(purgeKeyAppend) || tokenToSetName.endsWith(purgeKeyAppend) || setnamesToTokensKeyAppend.endsWith(purgeKeyAppend)) {
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
                setsBytesPipeline.push([this.redisCommandMEMORY, this.redisCommandMEMORYOptions, (this.keyPrefix + sortedSetName)]);//Sets Bytes Query
                setsBytesUpdatesPipeline.push([this.redisCommandZADD, (this.bytesKey + sortedSetName), "0", sortedSetName]);//Sets total Bytes
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
                const tokensForSetSerialized = await client.run([this.redisCommandHGET, (this.keyPrefix + setName + this.setnamesToTokensKeyAppend), setName]);
                const setNamesToQuery = JSON.parse(tokensForSetSerialized) as string[] || [];
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
                client.release(token);
            }
        }
        return returnObject;
    }

    async purgeBegin(lastUpsertElapsedTimeInSeconds: number | null, maximumCountThreshold: number | null, maximumBytesThreshold: bigint | null, pendingSortedSetsTimeoutInSeconds?: number, maxSortedSetsToRetrive?: number): Promise<IError<Map<string, ISortedStringData[]>>> {
        // const returnObject: IError<Map<string, Array<ISortedStringData>>> = { data: new Map<string, Array<ISortedStringData>>(), error: undefined };

        // //Query Pending sortedsets
        // const adjustedExpiryTime = Date.now() - (pendingSortedSetsTimeoutInSeconds * 1000);
        // const pendingSets: Array<string> = [];
        // let pendingIndex = sortingHelper.lt(this.pendingSets, { time: adjustedExpiryTime, token: "" }, this.pendingSetsCompareFunctions);
        // while (pendingIndex < this.pendingSets.length && pendingIndex > -1) {
        //     const token = this.pendingSets[pendingIndex].token;
        //     pendingSets.push(token);
        //     pendingIndex++;
        // }

        // //Query Time Partition
        // const timePartitions: Array<string> = [];
        // if (lastUpsertElapsedTimeInSeconds !== null) {
        //     const adjustedElapsedTime = Date.now() - (lastUpsertElapsedTimeInSeconds * 1000);
        //     let timeIndex = sortingHelper.lt(this.metaLastSetTime, { setTime: adjustedElapsedTime, name: "" }, this.metaTimeCompareFunction);
        //     while (timeIndex < this.metaLastSetTime.length && timeIndex > -1) {
        //         const name = this.metaLastSetTime[timeIndex].name;
        //         timePartitions.push(name);
        //         timeIndex++;
        //     }
        // }

        // //Query Count Partition
        // const countPartitions: Array<string> = [];
        // if (maximumCountThreshold !== null) {
        //     let countIndex = sortingHelper.gte(this.metaCount, { count: maximumCountThreshold, name: "" }, this.metaCountCompareFunction);
        //     while (countIndex < this.metaCount.length && countIndex > -1) {
        //         const name = this.metaCount[countIndex].name;
        //         countPartitions.push(name);
        //         countIndex++;
        //     }
        // }

        // //Query Bytes Partition
        // const bytesPartitions: Array<string> = [];
        // if (maximumBytesThreshold !== null) {
        //     let byteIndex = sortingHelper.gte(this.metaBytes, { bytes: maximumBytesThreshold, name: "" }, this.metaByteCompareFunction);
        //     while (byteIndex < this.metaBytes.length && byteIndex > -1) {
        //         const name = this.metaBytes[byteIndex].name;
        //         bytesPartitions.push(name);
        //         byteIndex++;
        //     }
        // }

        // //Combine partitions
        // const partitionsToDump: Array<string> = [...pendingSets, ...timePartitions, ...countPartitions, ...bytesPartitions];

        // //Dump partitions
        // let counter = 0;
        // while (counter < Math.min(partitionsToDump.length, maxSortedSetsToRetrive)) {
        //     const nameOrToken = partitionsToDump[counter];
        //     const setName = this.tokenToSetname.get(nameOrToken) || nameOrToken;
        //     const z = this.sets.get(nameOrToken) || new SortedSet();
        //     const results = z.rangeByScore(null, null, { withScores: true });
        //     const purgedSS = new SortedSet();
        //     const returnSetData = new Array<ISortedStringData>();
        //     results.forEach((zElement: string[]) => {
        //         returnSetData.push({ score: BigInt(zElement[1]), setName: setName, payload: zElement[0] });//
        //         purgedSS.add(zElement[0], BigInt(zElement[1]));
        //     });
        //     const byteIndex = this.metaBytes.findIndex(e => e.name === setName);
        //     const countIndex = this.metaCount.findIndex(e => e.name === setName);
        //     const lastEditedIndex = this.metaLastSetTime.findIndex(e => e.name === setName);
        //     if (byteIndex !== -1) {
        //         this.metaBytes.splice(byteIndex, 1);
        //     }
        //     if (countIndex !== -1) {
        //         this.metaCount.splice(countIndex, 1);
        //     }
        //     if (lastEditedIndex !== -1) {
        //         this.metaLastSetTime.splice(lastEditedIndex, 1);
        //     }

        //     let token = this.constructToken(setName);
        //     if (this.tokenToSetname.has(nameOrToken)) {//This means its token 
        //         this.tokenToSetname.delete(nameOrToken);
        //         const dataPurgeTokens = this.setnameToToken.get(setName) || [];
        //         const tokenIndex = dataPurgeTokens.findIndex(e => e === nameOrToken)
        //         const pendingIndex = this.pendingSets.findIndex(e => e.token === nameOrToken);
        //         if (tokenIndex !== -1) {
        //             dataPurgeTokens.splice(tokenIndex, 1);
        //         }
        //         if (pendingIndex !== -1) {
        //             this.pendingSets.splice(pendingIndex, 1);
        //         }
        //         this.setnameToToken.set(setName, dataPurgeTokens);
        //         token = this.constructToken(nameOrToken);
        //     }
        //     sortingHelper.add(this.pendingSets, { token: token, time: Date.now() }, this.pendingSetsCompareFunctions);

        //     this.tokenToSetname.set(token, setName);

        //     const dataPurgeTokens = this.setnameToToken.get(setName) || [];
        //     dataPurgeTokens.push(token);
        //     this.setnameToToken.set(setName, dataPurgeTokens);

        //     this.sets.delete(nameOrToken);
        //     this.sets.set(token, purgedSS);

        //     returnObject.data.set(token, returnSetData);
        //     counter++;
        // };

        // return Promise.resolve(returnObject);
        throw new Error("Method not implemented.");
    }

    purgeEnd(tokens: string[]): Promise<IBulkResponse<string[], IError<string>[]>> {
        throw new Error("Method not implemented.");
    }
}

export enum Operation {
    Read,
    Write,
    ReadNWrite
}