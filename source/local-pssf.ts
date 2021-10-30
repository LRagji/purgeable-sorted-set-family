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
    private pendingSets = new Array<{ token: string, time: number }>();
    private setnameToToken = new Map<string, Array<string>>();
    private tokenToSetname = new Map<string, string>();
    private metaByteCompareFunction = (lhs: { bytes: bigint; name: string; }, rhs: { bytes: bigint; name: string; }) => <1 | 0 | -1>Math.min(Math.max(parseInt((lhs.bytes - rhs.bytes).toString()), -1), 1);
    private metaCountCompareFunction = (lhs: { count: number; name: string; }, rhs: { count: number; name: string; }) => <1 | 0 | -1>Math.min(Math.max(lhs.count - rhs.count, -1), 1)
    private metaTimeCompareFunction = (lhs: { setTime: number; name: string; }, rhs: { setTime: number; name: string; }) => <1 | 0 | -1>Math.min(Math.max(lhs.setTime - rhs.setTime, -1), 1);
    private pendingSetsCompareFunctions = (lhs: { token: string, time: number }, rhs: { token: string, time: number }) => <1 | 0 | -1>Math.min(Math.max(lhs.time - rhs.time, -1), 1);
    private purgeKeyAppend: string;

    constructor(purgeKeyAppend = "purged") {
        this.purgeKeyAppend = purgeKeyAppend;
    }

    upsert(data: ISortedStringData[]): Promise<IBulkResponse<ISortedStringData[], IError<ISortedStringData>[]>> {
        const returnObject = { succeeded: new Array<ISortedStringData>(), failed: new Array<IError<ISortedStringData>>() };
        data.forEach(ss => {
            if (ss.score >= maximumScore || ss.score <= minimumScore) {
                returnObject.failed.push({ data: ss, error: new Error(`Score(${ss.score}) for set named "${ss.setName}" is not within range of ${minimumScore} to ${maximumScore}.`) });
            }
            else if (ss.setName.endsWith(this.purgeKeyAppend) === true) {
                returnObject.failed.push({ data: ss, error: new Error(`Setname "${ss.setName}" cannot end with system reserved key "${this.purgeKeyAppend}".`) });
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
                    metaBytesElement.bytes += ss.bytes || 0n;
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
            const setNames = this.setnameToToken.get(setName) || [];
            setNames.push(setName);
            if (setNames.length === 1) {
                const z = this.sets.get(setNames[0]) || new SortedSet();
                const results = z.rangeByScore(scoreStart, scoreEnd, { withScores: true });
                returnObject.data = results.map((e: Array<string>) => ({ score: BigInt(e[1]), setName: setNames[0], payload: e[0] }));
            }
            else {
                const unionSet = setNames.reduce((acc, nameOrToken) => {
                    const z = this.sets.get(nameOrToken) || new SortedSet();
                    const results = z.rangeByScore(scoreStart, scoreEnd, { withScores: true });
                    results.forEach((e: Array<string>) => acc.add(e[0], BigInt(e[1])));
                    return acc;
                }, new SortedSet());
                const results = unionSet.rangeByScore(scoreStart, scoreEnd, { withScores: true });
                returnObject.data = results.map((e: Array<Array<any>>) => ({ score: e[1], setName: setName, payload: e[0] }));
            }
        }
        return Promise.resolve(returnObject);
    }

    purgeBegin(lastUpsertElapsedTimeInSeconds: number | null, maximumCountThreshold: number | null, maximumBytesThreshold: bigint | null, pendingSortedSetsTimeoutInSeconds = 3600, maxSortedSetsToRetrive = 10): Promise<IError<Map<string, ISortedStringData[]>>> {
        const returnObject: IError<Map<string, Array<ISortedStringData>>> = { data: new Map<string, Array<ISortedStringData>>(), error: undefined };

        //Query Pending sortedsets
        const adjustedExpiryTime = Date.now() - (pendingSortedSetsTimeoutInSeconds * 1000);
        const purgableSets: Array<string> = [];
        let pendingIndex = sortingHelper.lt(this.pendingSets, { time: adjustedExpiryTime, token: "" }, this.pendingSetsCompareFunctions);
        while (pendingIndex < this.pendingSets.length && pendingIndex > -1 && purgableSets.length < maxSortedSetsToRetrive) {
            const token = this.pendingSets[pendingIndex].token;
            purgableSets.push(token);
            pendingIndex++;
        }

        //Query Elapsed sortedsets
        if (lastUpsertElapsedTimeInSeconds !== null && purgableSets.length < maxSortedSetsToRetrive) {
            const adjustedElapsedTime = Date.now() - (lastUpsertElapsedTimeInSeconds * 1000);
            let timeIndex = sortingHelper.lt(this.metaLastSetTime, { setTime: adjustedElapsedTime, name: "" }, this.metaTimeCompareFunction);
            while (timeIndex < this.metaLastSetTime.length && timeIndex > -1 && purgableSets.length < maxSortedSetsToRetrive) {
                const name = this.metaLastSetTime[timeIndex].name;
                purgableSets.push(name);
                timeIndex++;
            }
        }

        //Query Count sortedsets
        if (maximumCountThreshold !== null && purgableSets.length < maxSortedSetsToRetrive) {
            let countIndex = sortingHelper.gte(this.metaCount, { count: maximumCountThreshold, name: "" }, this.metaCountCompareFunction);
            while (countIndex < this.metaCount.length && countIndex > -1 && purgableSets.length < maxSortedSetsToRetrive) {
                const name = this.metaCount[countIndex].name;
                purgableSets.push(name);
                countIndex++;
            }
        }

        //Query Bytes sortedsets
        if (maximumBytesThreshold !== null && purgableSets.length < maxSortedSetsToRetrive) {
            let byteIndex = sortingHelper.gte(this.metaBytes, { bytes: maximumBytesThreshold, name: "" }, this.metaByteCompareFunction);
            while (byteIndex < this.metaBytes.length && byteIndex > -1 && purgableSets.length < maxSortedSetsToRetrive) {
                const name = this.metaBytes[byteIndex].name;
                purgableSets.push(name);
                byteIndex++;
            }
        }

        //Dump partitions
        let counter = 0;
        while (counter < purgableSets.length) {
            const nameOrToken = purgableSets[counter];
            const setName = this.tokenToSetname.get(nameOrToken) || nameOrToken;
            const z = this.sets.get(nameOrToken) || new SortedSet();
            const results = z.rangeByScore(null, null, { withScores: true });
            const purgedSS = new SortedSet();
            const returnSetData = new Array<ISortedStringData>();
            results.forEach((zElement: string[]) => {
                returnSetData.push({ score: BigInt(zElement[1]), setName: setName, payload: zElement[0] });//
                purgedSS.add(zElement[0], BigInt(zElement[1]));
            });
            const byteIndex = this.metaBytes.findIndex(e => e.name === setName);
            const countIndex = this.metaCount.findIndex(e => e.name === setName);
            const lastEditedIndex = this.metaLastSetTime.findIndex(e => e.name === setName);
            if (byteIndex !== -1) {
                this.metaBytes.splice(byteIndex, 1);
            }
            if (countIndex !== -1) {
                this.metaCount.splice(countIndex, 1);
            }
            if (lastEditedIndex !== -1) {
                this.metaLastSetTime.splice(lastEditedIndex, 1);
            }

            let token = this.constructToken(setName);
            if (setName !== nameOrToken) {//This means nameOrToken is token 
                this.tokenToSetname.delete(nameOrToken);
                const dataPurgeTokens = this.setnameToToken.get(setName) || [];
                const tokenIndex = dataPurgeTokens.findIndex(e => e === nameOrToken)
                const pendingIndex = this.pendingSets.findIndex(e => e.token === nameOrToken);
                if (tokenIndex !== -1) {
                    dataPurgeTokens.splice(tokenIndex, 1);
                }
                if (pendingIndex !== -1) {
                    this.pendingSets.splice(pendingIndex, 1);
                }
                this.setnameToToken.set(setName, dataPurgeTokens);
                token = this.constructToken(nameOrToken);
            }
            sortingHelper.add(this.pendingSets, { token: token, time: Date.now() }, this.pendingSetsCompareFunctions);

            this.tokenToSetname.set(token, setName);

            const dataPurgeTokens = this.setnameToToken.get(setName) || [];
            dataPurgeTokens.push(token);
            this.setnameToToken.set(setName, dataPurgeTokens);

            this.sets.delete(nameOrToken);
            this.sets.set(token, purgedSS);

            returnObject.data.set(token, returnSetData);
            counter++;
        };

        return Promise.resolve(returnObject);
    }

    purgeEnd(tokens: string[]): Promise<IBulkResponse<string[], IError<string>[]>> {
        const returnObject = { succeeded: new Array<string>(), failed: new Array<IError<string>>() };
        tokens.forEach(token => {
            const pendingIndex = this.pendingSets.findIndex(e => e.token === token);
            if (pendingIndex !== -1) {
                const setName = this.tokenToSetname.get(token) || "";
                const dataPurgeTokens = this.setnameToToken.get(setName) || [];
                const tokenIndex = dataPurgeTokens.findIndex(e => e === token)
                if (tokenIndex !== -1) {
                    dataPurgeTokens.splice(tokenIndex, 1);
                }
                this.setnameToToken.set(setName, dataPurgeTokens);
                this.tokenToSetname.delete(token);
                this.pendingSets.splice(pendingIndex, 1);
                this.sets.delete(token);
                returnObject.succeeded.push(token);
            }
            else {
                returnObject.failed.push({ data: token, error: new Error(`Token "${token}" doesnot exists.`) });
            }
        });
        return Promise.resolve(returnObject);
    }

    constructToken(sortedSetName: string): string {
        return `${sortedSetName}${this.purgeKeyAppend}`;
    }
}