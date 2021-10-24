import { IBulkResponse } from "./i-bulk-response";
import { IError } from "./i-error";

export interface ISortedStringData {
    score: bigint;
    bytes: bigint;
    setName: string;
    payload: string
}

export interface IPurgeableSortedSetFamily<T extends ISortedStringData> {

    upsert(data: T[]): Promise<IBulkResponse<T[], IError<T>[]>>;

    scoreRangeQuery(setName: string, scoreStart: bigint, scoreEnd: bigint): Promise<IError<T[]>>;

    purgeBegin(lastUpsertElapsedTimeInSeconds: number | null, maximumCountThreshold: number | null, maximumBytesThreshold: bigint | null, pendingSortedSetsTimeoutInSeconds?: number, maxSortedSetsToRetrive?: number): Promise<IError<T[]>>;

    purgeEnd(setNames: string[]): Promise<IBulkResponse<string[], IError<string>[]>>
}