import { IBulkResponse, IError, IPurgeableSortedSetFamily, ISortedStringData } from "./index";
import dimensionalHelper from "@stdlib/ndarray";
import Crypto from "crypto";

export class NDimensionalPartitionedSortedSet {
    private partitionShape: bigint[];
    private partitionNameSeperator: string;
    private partitionShapeInteger: number[];
    private shardResolver: (details: IPatitionDetails) => IPurgeableSortedSetFamily<ISortedStringData>;
    private settingsHash: string;

    constructor(partitionShape: bigint[], shardResolver: (details: IPatitionDetails) => IPurgeableSortedSetFamily<ISortedStringData>, partitionNameSeperator = '-') {
        if (partitionShape.length === 0) {
            throw new Error(`Invalid parameter "partitionShape" cannot be of length zero.`);
        }
        this.partitionShape = partitionShape;
        this.partitionShapeInteger = partitionShape.map(e => parseInt(e.toString()));
        this.partitionNameSeperator = partitionNameSeperator;
        this.shardResolver = shardResolver;
        this.settingsHash = this.objectHash({ "partitionNameSeperator": partitionNameSeperator, "partitionShape": partitionShape.join(partitionNameSeperator) });
    }

    async write(data: IDimentionalData[]): Promise<IBulkResponse<IDimentionalData[], IError<IDimentionalData>[]>> {
        const returnObject = { succeeded: new Array<IDimentionalData>(), failed: new Array<IError<IDimentionalData>>() };
        const partitionData = new Map<string, { data: ISortedStringData[], partitiondetails: IPatitionDetails, rawData: IDimentionalData[] }>();
        for (let index = 0; index < data.length; index++) {
            const element = data[index];
            if (element.dimensions.length !== this.partitionShape.length) {
                returnObject.failed.push({ error: new Error(`Data dimensions(${element.dimensions.length}) does not match partition shape(${this.partitionShape.length}) provided.`), data: element });
            }
            else {
                const partitionStart = this.partitionShape.map((ps, psIdx) => element.dimensions[psIdx] - (element.dimensions[psIdx] % ps));
                const partitionName = this.partitionNameBuilder(partitionStart);
                const relativeDimensions = partitionStart.map((ps, psIdx) => element.dimensions[psIdx] - ps);
                const score = BigInt(dimensionalHelper.sub2ind(this.partitionShapeInteger, ...relativeDimensions.map(e => parseInt(e.toString())), { order: 'row-major' }));
                const existingData = partitionData.get(partitionName) || { data: new Array<ISortedStringData>(), partitiondetails: { name: partitionName, startDimensions: partitionStart }, rawData: new Array<IDimentionalData>() };
                existingData.data.push({ score: score, setName: partitionName, payload: element.payload, bytes: element.bytes });
                existingData.rawData.push(element);
                partitionData.set(partitionName, existingData);
            }
        }
        const partitionNames = Array.from(partitionData.keys());
        for (let index = 0; index < partitionNames.length; index++) {
            const partitionName = partitionNames[index];
            const info = partitionData.get(partitionName)!;
            const shard = await this.shardResolver(info.partitiondetails);
            const results = await shard.upsert(info.data);
            returnObject.failed = results.failed.map(failedElement => {
                const failedIndex = info.rawData.findIndex(e => e.payload === failedElement.data.payload);
                if (failedIndex != -1) {
                    const failedItem = info.rawData.splice(failedIndex, 1)[0];
                    return { error: failedElement.error, data: failedItem };
                }
                else {
                    return { error: new Error(`Cannot find index for payload ${failedElement.data.payload} which has following error ${failedElement.error?.message}`), data: { payload: failedElement.data.payload, dimensions: [] } };
                }
            });
            returnObject.succeeded = returnObject.succeeded.concat(info.rawData);
        }
        return returnObject;
    }

    rangeRead(start: Array<bigint>, end: Array<bigint>): Promise<IError<IDimentionalData[]>> {
        throw new Error("Not implmented");
    }

    private partitionNameBuilder(partitionStart: bigint[]): string {
        return `${this.settingsHash}${this.partitionNameSeperator}${partitionStart.join(this.partitionNameSeperator)}`;
    }

    private objectHash(settings: object): string {
        return Crypto.createHash("sha256").update(JSON.stringify(settings), "binary").digest("hex");
    }
}

export interface IDimentionalData {
    payload: string,
    dimensions: bigint[]
    bytes?: bigint
}

export interface IPatitionDetails {
    name: string,
    startDimensions: bigint[]
}

export class Absolute {

    private max = (...args: bigint[]) => args.reduce((m, e) => e > m ? e : m);

    private min = (...args: bigint[]) => args.reduce((m, e) => e < m ? e : m);

    private frameStart(value: bigint, frameLength: bigint): bigint {
        return value - (value % frameLength);
    }

    private subsequentFrameStart(value: bigint, frameLength: bigint): bigint {
        return this.frameStart(value, frameLength) + frameLength;
    }

    private frameEnd(value: bigint, frameLength: bigint): bigint {
        return this.frameStart(value, frameLength) + (frameLength - 1n);
    }

    private async forLoop(start: bigint[], end: bigint[], stride: bigint[], callback: (iterator: bigint[], start: bigint[], end: bigint[], stride: bigint[]) => Promise<boolean>): Promise<void> {
        const lsd = 0;
        stride.forEach((e, idx) => {
            if (e <= 0) {
                throw new Error(`Stride has to be positive & non zero quantity. ${e} @Index:${idx}`);
            }
        });
        end.forEach((e, idx) => {
            if (e < start[idx]) {
                throw new Error(`Start dimension ${start[idx]} @Index:${idx} has to be smaller than end dimension ${e}`);
            }
        });
        let counter = Array.from(start);
        let overflow = false;
        let watchDog = BigInt(start.map((e, idx) => end[idx] - e).reduce((acc, e) => (e + 1n) * acc, 1n));
        let cancelled = false;
        do {
            while (counter[lsd] <= end[lsd] && watchDog >= BigInt(0) && cancelled === false) {
                cancelled = await callback(Array.from(counter), start, end, stride) || false;
                const nextVal = this.min(this.subsequentFrameStart(counter[lsd], stride[lsd]), end[lsd]);
                counter[lsd] = end[lsd] - counter[lsd] === 0n ? (counter[lsd] + 1n) : nextVal;
                watchDog--;
                if (watchDog < BigInt(0)) {
                    throw new Error("Navigation failed, Infinite Loop detected!!");
                }
            }
            for (let idx = 0; idx < counter.length; idx++) {
                if (!(counter[idx] <= end[idx])) {
                    if ((idx + 1) >= counter.length) {
                        overflow = true;
                    }
                    else {
                        counter[idx] = start[idx];
                        const nextVal = this.min(this.subsequentFrameStart(counter[idx + 1], stride[idx + 1]), end[idx + 1]);
                        counter[idx + 1] = end[idx + 1] - counter[idx + 1] === 0n ? (counter[idx + 1] + 1n) : nextVal;
                    }
                }
            };
        }
        while (overflow === false && watchDog >= BigInt(0) && cancelled === false)
    };

    async partitionedRanges(rangeStart: bigint[], rangeEnd: bigint[], rangeStrides: bigint[], partitionNameResolver: (vector: bigint[]) => string = (v) => v.join(",")): Promise<Map<string, bigint[][]>> {
        let ranges = new Map<string, bigint[][]>();
        await this.forLoop(rangeStart, rangeEnd, rangeStrides, (counter, start, end, stride) => {
            const frameStart = counter.map((e, idx) => this.frameStart(e, stride[idx]));
            const partitionName = partitionNameResolver(frameStart);
            const existingRange = ranges.get(partitionName) || [];
            if (existingRange.length === 0) {
                existingRange.push(counter);
                const endVector = counter.map((e, idx) => this.frameEnd(e, stride[idx]));
                existingRange.push(endVector);
            }
            else {
                const endVectorOrMaxVector = existingRange.pop() || [];
                const newVector = endVectorOrMaxVector.map((e, i) => this.min(this.max(this.frameEnd(e, stride[i]), counter[i], e), end[i]));
                existingRange.push(newVector);
            }
            ranges.set(partitionName, existingRange);
            return Promise.resolve(true);
        });
        return ranges;
    }
}