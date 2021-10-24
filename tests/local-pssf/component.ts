import * as assert from 'assert';
import { IPurgeableSortedSetFamily, ISortedStringData } from '../../source/i-purgeable-sorted-set';
import { LocalPSSF } from "../../source/local-pssf";

describe('"LocalPSSF" Set/Query component tests', () => {

    it('should be able to upsert and get string data', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const rangeResult = await target.scoreRangeQuery("Laukik", 1n, 3n);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(rangeResult.error, undefined);
        const readData = data.map(e => { e.bytes = 0n; return e });
        assert.deepStrictEqual(rangeResult.data, readData);
    });

    it('should be able to upsert and get updated data', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult1 = await target.upsert(data);
        data.push({ score: 4n, payload: "D", setName: "Laukik", bytes: 1n });
        data.push({ score: 5n, payload: "C", setName: "Laukik", bytes: 1n });
        const setResult2 = await target.upsert(data);
        const rangeResult = await target.scoreRangeQuery("Laukik", 1n, 5n);

        //Verify
        assert.deepStrictEqual(setResult2.failed.length, 0);
        assert.deepStrictEqual(setResult2.succeeded, data);
        assert.deepStrictEqual(rangeResult.error, undefined);
        assert.deepStrictEqual(rangeResult.data, [data[0], data[1], data[3], data[4]].map(e => { e.bytes = 0n; return e; }));
        assert.deepStrictEqual(setResult1.failed.length, 0);
        assert.deepStrictEqual(setResult1.succeeded, [data[0], data[1], data[2]]);
    });

    it('should only return data for a given range', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        //Test
        await target.upsert(data);
        const rangeResult = await target.scoreRangeQuery("Laukik", 1n, 2n);

        //Verify
        assert.deepStrictEqual(rangeResult.error, undefined);
        assert.deepStrictEqual(rangeResult.data, [data[0], data[1]].map(e => { e.bytes = 0n; return e; }));
    });

    it('should be inclusive on range reads', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        await target.upsert(data);
        const rangeResult = await target.scoreRangeQuery("Laukik", 2n, 3n);

        //Verify
        assert.deepStrictEqual(rangeResult.error, undefined);
        assert.deepStrictEqual(rangeResult.data.length, 2);
        assert.deepStrictEqual(rangeResult.data[0].payload, "B");
        assert.deepStrictEqual(rangeResult.data[1].payload, "C");
    });

    it('should not allow more then score limit upsert via upsert', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: -9007199254740993n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 9007199254740993n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 2);
        assert.deepStrictEqual(setResult.succeeded, [data[2]]);
        assert.deepStrictEqual(setResult.failed[0]?.error?.message, `Score(-9007199254740993) for set named "Laukik" is not within range of -9007199254740992 to 9007199254740992.`);
        assert.deepStrictEqual(setResult.failed[1]?.error?.message, `Score(9007199254740993) for set named "Laukik" is not within range of -9007199254740992 to 9007199254740992.`);
    });

    it('should be able to upsert data for different sorted sets', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "1", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "1", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "1", bytes: 1n });
        data.push({ score: 4n, payload: "A", setName: "2", bytes: 1n });
        data.push({ score: 5n, payload: "B", setName: "2", bytes: 1n });
        data.push({ score: 6n, payload: "C", setName: "2", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const rangeResult1 = await target.scoreRangeQuery("1", 1n, 3n);
        const rangeResult2 = await target.scoreRangeQuery("2", 1n, 10n);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(rangeResult1.data, [data[0], data[1], data[2]].map(e => { e.bytes = 0n; return e; }));
        assert.deepStrictEqual(rangeResult2.data, [data[3], data[4], data[5]].map(e => { e.bytes = 0n; return e; }));
    });

    it('should return correct results when queried outside smaller range', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 4n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 5n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 6n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const rangeResult = await target.scoreRangeQuery("Laukik", 1n, 3n);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(rangeResult.data.length, 0);
        assert.deepStrictEqual(rangeResult.error, undefined);
    });

    it('should return correct results when queried outside greater range', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 4n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 5n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 6n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const rangeResult = await target.scoreRangeQuery("Laukik", 7n, 10n);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(rangeResult.data.length, 0);
        assert.deepStrictEqual(rangeResult.error, undefined);
    });

    it('should return correct results when queried partially overlapping smaller range', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 4n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 5n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 6n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const rangeResult = await target.scoreRangeQuery("Laukik", 2n, 4n);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(rangeResult.data, [data[0]].map(e => { e.bytes = 0n; return e; }));
        assert.deepStrictEqual(rangeResult.error, undefined);
    });

    it('should return correct results when queried partially overlapping greater range', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 4n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 5n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 6n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const rangeResult = await target.scoreRangeQuery("Laukik", 6n, 10n);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded.length, 3);
        data.splice(0, 2);
        const readData = data.map(e => { e.bytes = 0n; return e });
        assert.deepStrictEqual(rangeResult.data, readData);
        assert.deepStrictEqual(rangeResult.error, undefined);
    });

    it('should return correct results when queried non existing sorted upsert', async () => {
        //Setup
        const target = testTarget();

        //Test
        const rangeResult = await target.scoreRangeQuery("Laukik", 6n, 10n);

        //Verify
        assert.deepStrictEqual(rangeResult.data.length, 0);
        assert.deepStrictEqual(rangeResult.error, undefined);
    });

    it('should return error when presented with reverse range', async () => {
        //Setup
        const target = testTarget();

        //Test
        const rangeResult = await target.scoreRangeQuery("Laukik", 10n, 6n);

        //Verify
        assert.deepStrictEqual(rangeResult.data.length, 0);
        assert.deepStrictEqual(rangeResult.error?.message, "Invalid range start(10) cannot be greator than end(6).");
    });

    it('should return query data in sequential ascending order', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 309n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 4n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 100n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const rangeResult = await target.scoreRangeQuery("Laukik", 0n, 500n);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(rangeResult.error, undefined);
        assert.deepStrictEqual(rangeResult.data, new Array({ score: 4n, payload: "B", setName: "Laukik", bytes: 0n },
            { score: 100n, payload: "C", setName: "Laukik", bytes: 0n }, { score: 309n, payload: "A", setName: "Laukik", bytes: 0n }));
    });
});

describe('"RamSortedSet" Purge component tests', () => {

    it('should not purge data when nothing is matching', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const purgeResult = await target.purgeBegin(null, null, null);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(purgeResult.error, undefined);
        assert.deepStrictEqual(purgeResult.data.length, 0);
    });

    it('should purge data when bytes have exceeded or equal', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        data.push({ score: 1n, payload: "A", setName: "small", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const purgeResult = await target.purgeBegin(null, null, 3n);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(purgeResult.error, undefined);
        assert.deepStrictEqual(purgeResult.data, [data[0], data[1], data[2]].map(e => { e.bytes = 0n; return e; }));
    });

    it('should not purge data when bytes have not exceeded', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const purgeResult = await target.purgeBegin(null, null, BigInt(Number.MAX_SAFE_INTEGER));

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(purgeResult.error, undefined);
        assert.deepStrictEqual(purgeResult.data.length, 0);
    });

    it('should purge data when count has exceeded or equal', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        data.push({ score: 1n, payload: "A", setName: "small", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const purgeResult = await target.purgeBegin(null, 3, null);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(purgeResult.error, undefined);
        assert.deepStrictEqual(purgeResult.data, [data[0], data[1], data[2]].map(e => { e.bytes = 0n; return e; }));
    });

    it('should not purge data when count has not exceeded', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const purgeResult = await target.purgeBegin(null, 10, null);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(purgeResult.error, undefined);
        assert.deepStrictEqual(purgeResult.data.length, 0);
    });

    it('should purge data when purge time has exceeded', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        await new Promise((acc, rej) => setTimeout(acc, 1500));//Kill time
        const purgeResult = await target.purgeBegin(1, null, null);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(purgeResult.error, undefined);
        assert.deepStrictEqual(purgeResult.data, [data[0], data[1], data[2]].map(e => { e.bytes = 0n; return e; }));
    }).timeout(-1);

    it('should not purge data when timeout has not exceeded', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const purgeResult = await target.purgeBegin(10, null, null);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(purgeResult.error, undefined);
        assert.deepStrictEqual(purgeResult.data.length, 0);
    });

    it('should not purge data when no condition is met.', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const purgeResult = await target.purgeBegin(10, 10, 1000n);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);
        assert.deepStrictEqual(purgeResult.error, undefined);
        assert.deepStrictEqual(purgeResult.data.length, 0);
    });

    it('should purge data when from unfinished list first', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        const freshData = new Array<ISortedStringData>();
        freshData.push({ score: 1n, payload: "D", setName: "Fresh", bytes: 1n });
        freshData.push({ score: 2n, payload: "E", setName: "Fresh", bytes: 1n });
        freshData.push({ score: 3n, payload: "F", setName: "Fresh", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const timedoutPurgeResult = await target.purgeBegin(null, 1, null);
        await new Promise((acc, rej) => setTimeout(acc, 1500));//Kill time
        const freshSetResult = await target.upsert(freshData);
        const purgeResult = await target.purgeBegin(null, 100, null, 1);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);

        assert.deepStrictEqual(timedoutPurgeResult.error, undefined);
        assert.deepStrictEqual(timedoutPurgeResult.data, [data[0], data[1], data[2]].map(e => { e.bytes = 0n; return e; }));

        assert.deepStrictEqual(freshSetResult.failed.length, 0);
        assert.deepStrictEqual(freshSetResult.succeeded, freshData);

        assert.deepStrictEqual(purgeResult.error, undefined);
        assert.deepStrictEqual(timedoutPurgeResult.data, [data[0], data[1], data[2]].map(e => { e.bytes = 0n; return e; }));

    }).timeout(-1);

    it('should not return purged finished data', async () => {
        //Setup
        const target = testTarget();
        const data = new Array<ISortedStringData>();
        data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        const freshData = new Array<ISortedStringData>();
        freshData.push({ score: 1n, payload: "D", setName: "Fresh", bytes: 1n });
        freshData.push({ score: 2n, payload: "E", setName: "Fresh", bytes: 1n });
        freshData.push({ score: 3n, payload: "F", setName: "Fresh", bytes: 1n });

        //Test
        const setResult = await target.upsert(data);
        const timedoutPurgeResult = await target.purgeBegin(null, 1, null);
        await new Promise((acc, rej) => setTimeout(acc, 1500));//Kill time
        const purgeFinishedResult = await target.purgeEnd(["Laukik"]);
        const freshSetResult = await target.upsert(freshData);
        const purgeResult = await target.purgeBegin(null, 100, null, 1);

        //Verify
        assert.deepStrictEqual(setResult.failed.length, 0);
        assert.deepStrictEqual(setResult.succeeded, data);

        assert.deepStrictEqual(timedoutPurgeResult.error, undefined);
        assert.deepStrictEqual(timedoutPurgeResult.data, [data[0], data[1], data[2]].map(e => { e.bytes = 0n; return e; }));

        assert.deepStrictEqual(purgeFinishedResult.succeeded, ["Laukik"]);
        assert.deepStrictEqual(purgeFinishedResult.failed.length, 0);

        assert.deepStrictEqual(freshSetResult.failed.length, 0);
        assert.deepStrictEqual(freshSetResult.succeeded, freshData);

        assert.deepStrictEqual(purgeResult.error, undefined);
        assert.deepStrictEqual(purgeResult.data.length, 0);

    }).timeout(-1);

    it('purge finish should return false when invalid sorted upsert is mentioned.', async () => {
        //Setup
        const target = testTarget();

        //Test
        const purgeFinishResult = await target.purgeEnd(["ABC"]);

        //Verify
        assert.deepStrictEqual(purgeFinishResult.succeeded.length, 0);
        assert.deepStrictEqual(purgeFinishResult.failed.length, 1);
        assert.deepStrictEqual(purgeFinishResult.failed[0].error?.message, 'Sorted set with "ABC" name doesnot exists.');
    });
});

function testTarget(): IPurgeableSortedSetFamily<ISortedStringData> {
    return new LocalPSSF();
}