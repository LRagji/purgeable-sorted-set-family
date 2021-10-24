import { IBulkResponse } from "./i-bulk-response";
import { IError } from "./i-error";
import { IPurgeableSortedSetFamily, ISortedStringData } from "./i-purgeable-sorted-set";
import * as sortingHelper from "sorted-array-functions"
// @ts-ignore
import SortedSet from "redis-sorted-set";

const minimumScore = -9007199254740992n;
const maximumScore = 9007199254740992n;

export class LocalPSSF implements IPurgeableSortedSetFamily<ISortedStringData> {
    private sets = new Map<string, SortedSet>();
    private metaBytes = new Array<{ bytes: bigint, name: string }>();
    private metaCount = new Array<{ count: number, name: string }>();
    private metaLastSetTime = new Array<{ setTime: number, name: string }>();
    private pendingSets = new Array<{ name: string, data: ISortedStringData[], time: number }>();
    private metaByteCompareFunction = (lhs: { bytes: bigint; name: string; }, rhs: { bytes: bigint; name: string; }) => <1 | 0 | -1>Math.min(Math.max(parseInt((lhs.bytes - rhs.bytes).toString()), -1), 1);
    private metaCountCompareFunction = (lhs: { count: number; name: string; }, rhs: { count: number; name: string; }) => <1 | 0 | -1>Math.min(Math.max(lhs.count - rhs.count, -1), 1)
    private metaTimeCompareFunction = (lhs: { setTime: number; name: string; }, rhs: { setTime: number; name: string; }) => <1 | 0 | -1>Math.min(Math.max(lhs.setTime - rhs.setTime, -1), 1);
    private pendingSetsCompareFunctions = (lhs: { name: string, data: ISortedStringData[], time: number }, rhs: { name: string, data: ISortedStringData[], time: number }) => <1 | 0 | -1>Math.min(Math.max(lhs.time - rhs.time, -1), 1);

    upsert(data: ISortedStringData[]): Promise<IBulkResponse<ISortedStringData[], IError<ISortedStringData>[]>> {
        const returnObject = { succeeded: new Array<ISortedStringData>(), failed: new Array<IError<ISortedStringData>>() };
        data.forEach(ss => {
            if (ss.score >= maximumScore || ss.score <= minimumScore) {
                returnObject.failed.push({ data: ss, error: new Error(`Score(${ss.score}) for set named "${ss.setName}" is not within range of ${minimumScore} to ${maximumScore}.`) });
            }
            else {
                const z = this.sets.get(ss.setName) || new SortedSet();
                const isUpdate = z.has(ss.payload);
                z.add(ss.payload, ss.score);
                this.sets.set(ss.setName, z);

                if (isUpdate === false) {
                    //Byte computations
                    const byteIndex = this.metaBytes.findIndex(e => e.name === ss.setName);
                    let metaBytesElement = { name: ss.setName, bytes: 0n };
                    if (byteIndex > -1) {
                        metaBytesElement = this.metaBytes.splice(byteIndex, 1)[0];
                    }
                    metaBytesElement.bytes += ss.bytes;
                    sortingHelper.add(this.metaBytes, metaBytesElement, this.metaByteCompareFunction);

                    //Count computations
                    const countIndex = this.metaCount.findIndex(e => e.name === ss.setName);
                    let metaCountElement = { name: ss.setName, count: 0 };
                    if (countIndex > -1) {
                        metaCountElement = this.metaCount.splice(countIndex, 1)[0];
                    }
                    metaCountElement.count += 1;
                    sortingHelper.add(this.metaCount, metaCountElement, this.metaCountCompareFunction);
                }
                //Time computations
                const timeIndex = this.metaLastSetTime.findIndex(e => e.name === ss.setName);
                let metaTimeElement = { name: ss.setName, setTime: 0 };
                if (timeIndex > -1) {
                    metaTimeElement = this.metaLastSetTime.splice(timeIndex, 1)[0];
                }
                metaTimeElement.setTime = Date.now();
                sortingHelper.add(this.metaLastSetTime, metaTimeElement, this.metaTimeCompareFunction);

                returnObject.succeeded.push(ss);
            }
        });
        return Promise.resolve(returnObject);
    }

    scoreRangeQuery(setName: string, scoreStart: bigint, scoreEnd: bigint): Promise<IError<ISortedStringData[]>> {
        const returnObject: IError<ISortedStringData[]> = { data: new Array<ISortedStringData>(), error: undefined };
        if (scoreStart > scoreEnd) {
            returnObject.error = new Error(`Invalid range start(${scoreStart}) cannot be greator than end(${scoreEnd}).`)
        }
        else {
            const z = this.sets.get(setName) || new SortedSet();
            const results = z.rangeByScore(scoreStart, scoreEnd, { withScores: true });
            returnObject.data = results.map((e: Array<Array<any>>) => ({ score: e[1], bytes: 0n, setName: setName, payload: e[0] }));
        }
        return Promise.resolve(returnObject);
    }

    purgeBegin(lastUpsertElapsedTimeInSeconds: number | null, maximumCountThreshold: number | null, maximumBytesThreshold: bigint | null, pendingSortedSetsTimeoutInSeconds = 3600, maxSortedSetsToRetrive = 10): Promise<IError<ISortedStringData[]>> {
        const returnObject: IError<Array<ISortedStringData>> = { data: new Array<ISortedStringData>(), error: undefined };

        //Query Pending sortedsets
        const adjustedExpiryTime = Date.now() - (pendingSortedSetsTimeoutInSeconds * 1000);
        const pendingSets: Array<string> = [];
        let pendingIndex = sortingHelper.lt(this.pendingSets, { time: adjustedExpiryTime, name: "", data: new Array<ISortedStringData>() }, this.pendingSetsCompareFunctions);
        while (pendingIndex < this.pendingSets.length && pendingIndex > -1) {
            const name = this.pendingSets[pendingIndex].name;
            pendingSets.push(name);
            pendingIndex++;
        }

        //Query Time Partition
        const timePartitions: Array<string> = [];
        if (lastUpsertElapsedTimeInSeconds !== null) {
            const adjustedElapsedTime = Date.now() - (lastUpsertElapsedTimeInSeconds * 1000);
            let timeIndex = sortingHelper.lt(this.metaLastSetTime, { setTime: adjustedElapsedTime, name: "" }, this.metaTimeCompareFunction);
            while (timeIndex < this.metaLastSetTime.length && timeIndex > -1) {
                const name = this.metaLastSetTime[timeIndex].name;
                timePartitions.push(name);
                timeIndex++;
            }
        }

        //Query Count Partition
        const countPartitions: Array<string> = [];
        if (maximumCountThreshold !== null) {
            let countIndex = sortingHelper.gte(this.metaCount, { count: maximumCountThreshold, name: "" }, this.metaCountCompareFunction);
            while (countIndex < this.metaCount.length && countIndex > -1) {
                const name = this.metaCount[countIndex].name;
                countPartitions.push(name);
                countIndex++;
            }
        }

        //Query Bytes Partition
        const bytesPartitions: Array<string> = [];
        if (maximumBytesThreshold !== null) {
            let byteIndex = sortingHelper.gte(this.metaBytes, { bytes: maximumBytesThreshold, name: "" }, this.metaByteCompareFunction);
            while (byteIndex < this.metaBytes.length && byteIndex > -1) {
                const name = this.metaBytes[byteIndex].name;
                bytesPartitions.push(name);
                byteIndex++;
            }
        }

        //Combine partitions
        const partitionsToDump: Array<string> = [...pendingSets, ...timePartitions, ...countPartitions, ...bytesPartitions];

        //Dump partitions
        let counter = 0;
        while (counter < Math.min(partitionsToDump.length, maxSortedSetsToRetrive)) {
            const name = partitionsToDump[counter];
            const z = this.sets.get(name) || new SortedSet();
            const results = z.rangeByScore(null, null, { withScores: true });
            const setData = results.map((e: Array<Array<any>>) => ({ score: e[1], bytes: 0n, setName: name, payload: e[0] }));
            returnObject.data = returnObject.data.concat(setData);
            const byteIndex = this.metaBytes.findIndex(e => e.name === name);
            this.metaBytes.splice(byteIndex, 1);
            const countIndex = this.metaCount.findIndex(e => e.name === name);
            this.metaCount.splice(countIndex, 1);
            const lastEditedIndex = this.metaLastSetTime.findIndex(e => e.name === name);
            this.metaLastSetTime.splice(lastEditedIndex, 1);
            const pendingIndex = this.pendingSets.findIndex(e => e.name === name);
            if (pendingIndex !== -1) {
                this.pendingSets.splice(pendingIndex, 1);
            }
            sortingHelper.add(this.pendingSets, { name: name, data: setData, time: Date.now() }, this.pendingSetsCompareFunctions);
            counter++;
        };

        return Promise.resolve(returnObject);
    }

    purgeEnd(setNames: string[]): Promise<IBulkResponse<string[], IError<string>[]>> {
        const returnObject = { succeeded: new Array<string>(), failed: new Array<IError<string>>() };
        setNames.forEach(name => {
            const pendingIndex = this.pendingSets.findIndex(e => e.name === name);
            if (pendingIndex !== -1) {
                this.pendingSets.splice(pendingIndex, 1);
                returnObject.succeeded.push(name);
            }
            else {
                returnObject.failed.push({ data: name, error: new Error(`Sorted set with "${name}" name doesnot exists.`) });
            }
        });
        return Promise.resolve(returnObject);
    }
}