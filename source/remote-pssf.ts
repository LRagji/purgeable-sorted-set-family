import { IPurgeableSortedSetFamily, ISortedStringData } from ".";
import { IBulkResponse } from "./i-bulk-response";
import { IError } from "./i-error";
import { IRedisClient } from "./i-redis-client";

const minimumScore = -9007199254740992n;
const maximumScore = 9007199254740992n;

export class RemotePSSF implements IPurgeableSortedSetFamily<ISortedStringData> {
    private redisClientResolver: (operation: Operation) => Promise<IRedisClient>;
    private activityKey: string;
    private keyPrefix: string;
    private countKey: string;
    private bytesKey: string;
    private purgeKeyAppend: string;

    constructor(redisClientResolver: (operation: Operation) => Promise<IRedisClient>, keyPrefix: string = "", purgeKeyAppend: string = "purged",
        activityKey: string = "Activity", countKey: string = "Stats", bytesKey: string = "Bytes") {
        this.redisClientResolver = redisClientResolver;
        this.keyPrefix = keyPrefix;
        this.activityKey = activityKey;
        this.countKey = countKey;
        this.bytesKey = bytesKey;
        this.purgeKeyAppend = purgeKeyAppend;
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
            else {
                const commands = zaddCommands.get(ss.setName) || new Array<string>();
                commands.push(ss.score.toString(), ss.payload);
                zaddCommands.set(ss.setName, commands);
                returnObject.succeeded.push(ss);
            }
        });
        const client = await this.redisClientResolver(Operation.ReadNWrite);
        try {
            const setsPipeline = new Array<Array<string>>();
            const setsBytesPipeline = new Array<Array<string>>();
            let setsBytesUpdatesPipeline = new Array<Array<string>>();
            zaddCommands.forEach((values, sortedSetName) => {
                setsPipeline.push(["ZADD", (this.keyPrefix + sortedSetName), ...values],//Main Sorted Set
                    ["ZADD", (this.keyPrefix + this.activityKey), (Date.now() / 1000).toFixed(0), sortedSetName], //Sets Activity Time
                    ["ZINCRBY", (this.keyPrefix + this.countKey), (values.length / 2).toFixed(0), sortedSetName]); //Sets Count
                setsBytesPipeline.push(["MEMORY", "USAGE", (this.keyPrefix + sortedSetName)]);//Sets Bytes Query
                setsBytesUpdatesPipeline.push(["ZADD", (this.bytesKey + sortedSetName), "0", sortedSetName]);//Sets total Bytes
            });
            await client.acquire();
            await client.pipeline(setsPipeline);
            const bytesResults: string[] = await client.pipeline(setsBytesPipeline);
            setsBytesUpdatesPipeline = setsBytesUpdatesPipeline.map((e, idx) => { e[2] = bytesResults[idx]; return e; });
            await client.pipeline(setsBytesUpdatesPipeline);
        }
        finally {
            await client.release();
        }
        return Promise.resolve(returnObject);
    }

    scoreRangeQuery(setName: string, scoreStart: bigint, scoreEnd: bigint): Promise<IError<ISortedStringData[]>> {
        throw new Error("Method not implemented.");
    }

    purgeBegin(lastUpsertElapsedTimeInSeconds: number | null, maximumCountThreshold: number | null, maximumBytesThreshold: bigint | null, pendingSortedSetsTimeoutInSeconds?: number, maxSortedSetsToRetrive?: number): Promise<IError<Map<string, ISortedStringData[]>>> {
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