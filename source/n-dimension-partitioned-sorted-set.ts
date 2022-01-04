import { IBulkResponse, IError, IPurgeableSortedSetFamily, ISortedStringData } from "./index";
import dimensionalHelper from "@stdlib/ndarray";
import Crypto from "crypto";
import * as sortingHelper from "sorted-array-functions"

export class NDimensionalPartitionedSortedSet {
    private partitionShape: bigint[];
    private partitionNameSeperator: string;
    private partitionShapeInteger: number[];
    private shardResolver: (details: IPatitionDetails) => Promise<IPurgeableSortedSetFamily<ISortedStringData>>;
    private settingsHash: string;
    private nDimensionCompareFunction = (end: number[]) => ((lhs: IDimentionalData, rhs: IDimentionalData) => <1 | 0 | -1>Math.min(Math.max(parseInt((this.dimensionToLinearIndex(lhs.dimensions, end) - this.dimensionToLinearIndex(rhs.dimensions, end)).toString()), -1), 1));

    constructor(partitionShape: bigint[], shardResolver: (details: IPatitionDetails) => Promise<IPurgeableSortedSetFamily<ISortedStringData>>, partitionNameSeperator = '-') {
        if (partitionShape.length === 0) {
            throw new Error(`Invalid parameter "partitionShape" cannot be of length zero.`);
        }
        this.partitionShape = partitionShape;
        this.partitionShapeInteger = partitionShape.map(e => parseInt(e.toString()));
        this.partitionNameSeperator = partitionNameSeperator;
        this.shardResolver = async (details: IPatitionDetails) => {
            const shard = await shardResolver(details);
            if (shard.purgeMarker() === partitionNameSeperator || shard.purgeMarker().replace(this.partitionNameSeperator, "") !== shard.purgeMarker()) {
                throw new Error(`Shard has a "purgeKeyAppend"(${shard.purgeMarker()}) which contains "partitionNameSeperator"(${this.partitionNameSeperator}) which is not allowed.`);
            }
            return shard;
        };
        this.settingsHash = this.objectHash({ "partitionNameSeperator": partitionNameSeperator, "partitionShape": partitionShape.join(partitionNameSeperator) });
    }

    public hash(): string {
        return this.settingsHash
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
                const score = this.dimensionToLinearIndex(relativeDimensions);
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

    async rangeRead(start: Array<bigint>, end: Array<bigint>, maxRanges: number = -1): Promise<IError<IDimentionalData[]>> {
        const returnObject: IError<IDimentionalData[]> = { data: new Array<IDimentionalData>(), error: undefined };
        const ranges = await Absolute.partitionedRanges(start, end, this.partitionShape, maxRanges);
        const processingHandles: Promise<IDimentionalData[]>[] = [];
        ranges.forEach((range, partitionStart) => processingHandles.push(this.readParitionRange(partitionStart, range[0], range[1])));
        const rawData = await Promise.allSettled(processingHandles);
        let errorString = "";
        returnObject.data = rawData.reduce((acc, rData) => {
            if (rData.status === "fulfilled") {
                //Reason for reverse is cause we need to invert insertions for same value.
                rData.value.reverse().forEach(v => sortingHelper.add(acc, v, this.nDimensionCompareFunction(end.map(e => parseInt((e + 1n).toString())))));
            }
            else {
                errorString += rData.reason.message;
            }
            return acc;
        }, returnObject.data);

        if (errorString != "") {
            returnObject.error = new Error(errorString);
        }

        return returnObject;
    }

    parseTokenizedDimentionalData(tokenizedData: Map<string, ISortedStringData[]>): Map<string, IDimentionalData[]> {
        const returnMap = new Map<string, IDimentionalData[]>();
        tokenizedData.forEach((data, token) => returnMap.set(token, this.parseDimensionalData(this.partitionNameDeconstructor(token), data)));
        return returnMap;
    }

    private async readParitionRange(partitionStart: bigint[], start: bigint[], end: bigint[]): Promise<IDimentionalData[]> {
        const relativePositionMapResolver = (e: bigint, idx: number) => e - partitionStart[idx];
        const localStart = this.dimensionToLinearIndex(start.map(relativePositionMapResolver));
        const localEnd = this.dimensionToLinearIndex(end.map(relativePositionMapResolver));
        const partitionName = this.partitionNameBuilder(partitionStart);
        const shard = await this.shardResolver({ name: partitionName, startDimensions: partitionStart });
        const result = await shard.scoreRangeQuery(partitionName, localStart, localEnd);
        if (result.error != undefined) {
            throw result.error;
        }
        return this.parseDimensionalData(partitionStart, result.data);
    }

    private partitionNameBuilder(partitionStart: bigint[]): string {
        return `${this.settingsHash}${this.partitionNameSeperator}${partitionStart.join(this.partitionNameSeperator)}`;
    }

    private partitionNameDeconstructor(partitionName: string): bigint[] {
        const decomposed = partitionName.split(this.partitionNameSeperator);
        if (decomposed.length < (this.partitionShapeInteger.length + 1)) {
            throw new Error(`Invalid partition name: ${partitionName}, expecting ${this.partitionShapeInteger.length + 1} dimensions seperated by "${this.partitionNameSeperator}".`)
        }
        if (decomposed[0] !== this.settingsHash) {
            throw new Error(`Invalid version, settings must have changed expected:"${this.settingsHash}" but was "${decomposed[0]}"`);
        }
        return decomposed.splice(1, this.partitionShapeInteger.length).map(e => BigInt(parseInt(e, 10)));
    }

    private parseDimensionalData(absolutePartitionStart: bigint[], sortedData: ISortedStringData[]): IDimentionalData[] {
        return sortedData.map(e => {
            const calculatedDimensions = this.linearIndexToDimension(e.score).map((e, idx) => absolutePartitionStart[idx] + e);
            if (e.bytes == undefined) {
                return {
                    "dimensions": calculatedDimensions,
                    "payload": e.payload,
                };
            }
            else {
                return {
                    "dimensions": calculatedDimensions,
                    "payload": e.payload,
                    "bytes": e.bytes
                };
            }
        });
    }

    private objectHash(settings: object): string {
        return Crypto.createHash("sha256").update(JSON.stringify(settings), "binary").digest("hex");
    }

    private dimensionToLinearIndex(dimensions: bigint[], shape: number[] = this.partitionShapeInteger): bigint {
        const indexInteger = dimensionalHelper.sub2ind(shape, ...dimensions.map(e => parseInt(e.toString())), { order: 'column-major' });
        return BigInt(indexInteger);
    }

    private linearIndexToDimension(linearIndex: bigint, shape: number[] = this.partitionShapeInteger): bigint[] {
        return dimensionalHelper.ind2sub(shape, parseInt(linearIndex.toString()), { order: 'column-major' }).map(e => BigInt(e));
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

class Absolute {

    private static max = (...args: bigint[]) => args.reduce((m, e) => e > m ? e : m);

    private static min = (...args: bigint[]) => args.reduce((m, e) => e < m ? e : m);

    private static frameStart(value: bigint, frameLength: bigint): bigint {
        return value - (value % frameLength);
    }

    private static subsequentFrameStart(value: bigint, frameLength: bigint): bigint {
        return this.frameStart(value, frameLength) + frameLength;
    }

    private static frameEnd(value: bigint, frameLength: bigint): bigint {
        return this.frameStart(value, frameLength) + (frameLength - 1n);
    }

    private static async forLoop(start: bigint[], end: bigint[], stride: bigint[], callback: (iterator: bigint[], start: bigint[], end: bigint[], stride: bigint[]) => Promise<boolean>): Promise<void> {
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

    static async partitionedRanges(rangeStart: bigint[], rangeEnd: bigint[], rangeStrides: bigint[], maxNumberOfPartitions: number = -1): Promise<Map<bigint[], bigint[][]>> {
        let ranges = new Map<string, bigint[][]>();
        const partitionNameConverter: (vector: bigint[]) => string = (v) => v.join(",");
        const partitionNameUnConverter: (partitionName: string) => bigint[] = (pn) => pn.split(",").map(e => BigInt(e));
        await this.forLoop(rangeStart, rangeEnd, rangeStrides, (counter, start, end, stride) => {
            const frameStart = counter.map((e, idx) => this.frameStart(e, stride[idx]));
            const partitionName = partitionNameConverter(frameStart);
            const existingRange = ranges.get(partitionName) || [];
            if (existingRange.length === 0) {
                existingRange.push(counter);
                const endVector = counter.map((e, idx) => this.min(this.frameEnd(e, stride[idx]), end[idx]));
                existingRange.push(endVector);
            }
            else {
                const endVectorOrMaxVector = existingRange.pop() || [];
                const newVector = endVectorOrMaxVector.map((e, i) => this.min(this.max(this.frameEnd(e, stride[i]), counter[i], e), end[i]));
                existingRange.push(newVector);
            }
            ranges.set(partitionName, existingRange);
            return Promise.resolve(ranges.size === maxNumberOfPartitions);
        });
        const returnObject = new Map<bigint[], bigint[][]>();
        ranges.forEach((v, k) => returnObject.set(partitionNameUnConverter(k), v));
        return returnObject;
    }
}