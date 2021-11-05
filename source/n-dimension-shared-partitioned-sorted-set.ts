import { IBulkResponse, IError, IPurgeableSortedSetFamily, ISortedStringData } from "./index";
import dimensionalHelper from "@stdlib/ndarray";

export class NDimensionalSharedPartitionedSortedSet {
    private partitionShape: bigint[];
    private partitionNameSeperator: string;
    private partitionShapeInterger: number[];
    private shardResolver: (details: IPatitionDetails) => IPurgeableSortedSetFamily<ISortedStringData>;

    constructor(partitionShape: bigint[], shardResolver: (details: IPatitionDetails) => IPurgeableSortedSetFamily<ISortedStringData>, partitionNameSeperator = '-') {
        if (partitionShape.length === 0) {
            throw new Error(`Invalid parameter "partitionShape" cannot be of length zero.`);
        }
        this.partitionShape = partitionShape;
        this.partitionShapeInterger = partitionShape.map(e => parseInt(e.toString()));
        this.partitionNameSeperator = partitionNameSeperator;
        this.shardResolver = shardResolver;
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
                const score = BigInt(dimensionalHelper.sub2ind(this.partitionShapeInterger, ...relativeDimensions.map(e => parseInt(e.toString())), { order: 'row-major' }));
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
            results.succeeded.map(e => ({ payload: e.payload, dimen }))
        }
        return returnObject;
    }

    partitionNameBuilder(partitionStart: bigint[]): string {
        return partitionStart.join(this.partitionNameSeperator);
    }

    rangeRead(start: Array<bigint>, end: Array<bigint>): Promise<IError<IDimentionalData[]>> {
        throw new Error("Not implmented");
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