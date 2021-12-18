import * as assert from 'assert';
import { IPurgeableSortedSetFamily, IRedisClient, ISortedStringData, LocalPSSF, RemotePSSF, NDimensionalPartitionedSortedSet } from '../../dist/index';
import { IDimentionalData } from '../../dist/n-dimension-partitioned-sorted-set';
import { RedisClient } from '../utilities/redis-client'
const purgeName = "Pur";
let client: IRedisClient;
var runs = [
    { createPSSF: (purkeyKey = purgeName): IPurgeableSortedSetFamily<ISortedStringData> => new LocalPSSF(purkeyKey), type: "NPSS Local" },
    { createPSSF: createRemotePsff, type: "NSPPS Remote" },
];

runs.forEach(function (run) {
    describe(`"${run.type}" Set/Query component tests`, () => {

        beforeEach(async function () {
            client = new RedisClient(process.env.REDISCON as string);
            await client.run(["FLUSHALL"]);
        });

        afterEach(async function () {
            await client.shutdown();
        });

        it('should be able to insert and get string data', async () => {
            //Setup
            const testShard = await run.createPSSF();
            const target = new NDimensionalPartitionedSortedSet([10n, 10n, 10n], (details) => Promise.resolve(testShard));
            const data = new Array<IDimentionalData>();
            data.push({ dimensions: [1n, 0n, 0n], payload: "A", bytes: 1n });
            data.push({ dimensions: [11n, 0n, 0n], payload: "B", bytes: 1n });
            data.push({ dimensions: [21n, 0n, 0n], payload: "C", bytes: 1n });

            //Test
            const setResult = await target.write(data);
            const rangeResult = await target.rangeRead([0n, 0n, 0n], [100n, 100n, 100n]);

            //Verify
            assert.deepStrictEqual(setResult.failed.length, 0);
            assert.deepStrictEqual(setResult.succeeded, data);
            assert.deepStrictEqual(rangeResult.error, undefined);
            const readData = data.map(e => { delete e.bytes; return e });
            assert.deepStrictEqual(rangeResult.data, readData);
        });

        it('should be able to insert and get string data in same sequence with same score', async () => {
            //Setup
            const testShard = run.createPSSF();
            const target = new NDimensionalPartitionedSortedSet([10n, 10n, 10n], (details) => Promise.resolve(testShard));
            const data = new Array<IDimentionalData>();
            data.push({ dimensions: [1n, 0n, 0n], payload: "A", bytes: 1n });
            data.push({ dimensions: [11n, 0n, 0n], payload: "B", bytes: 1n });
            data.push({ dimensions: [21n, 0n, 0n], payload: "C", bytes: 1n });


            //Test
            const setResult1 = await target.write(data);
            data.push({ dimensions: [21n, 0n, 0n], payload: "D", bytes: 1n });
            data.push({ dimensions: [21n, 0n, 0n], payload: "E", bytes: 1n });
            const setResult2 = await target.write(data);
            const rangeResult = await target.rangeRead([0n, 0n, 0n], [100n, 100n, 100n]);

            //Verify
            assert.deepStrictEqual(setResult1.failed.length, 0);
            assert.deepStrictEqual(setResult1.succeeded, [data[0], data[1], data[2]]);
            assert.deepStrictEqual(setResult2.failed.length, 0);
            assert.deepStrictEqual(setResult2.succeeded, data);
            assert.deepStrictEqual(rangeResult.error, undefined);
            const readData = data.map(e => { delete e.bytes; return e });
            assert.deepStrictEqual(rangeResult.data, readData);
        });

        it('should be able to update and get string data in same sequence with same score for the same partition', async () => {
            //Setup
            const testShard = run.createPSSF();
            const target = new NDimensionalPartitionedSortedSet([10n, 10n, 10n], (details) => Promise.resolve(testShard));
            const data = new Array<IDimentionalData>();
            data.push({ dimensions: [1n, 0n, 0n], payload: "A", bytes: 1n });
            data.push({ dimensions: [11n, 0n, 0n], payload: "B", bytes: 1n });
            data.push({ dimensions: [21n, 0n, 0n], payload: "C", bytes: 1n });


            //Test
            const setResult1 = await target.write(data);
            data.push({ dimensions: [22n, 0n, 0n], payload: "C", bytes: 1n });
            data.push({ dimensions: [21n, 0n, 0n], payload: "E", bytes: 1n });
            const setResult2 = await target.write(data);
            const rangeResult = await target.rangeRead([0n, 0n, 0n], [100n, 100n, 100n]);

            //Verify
            assert.deepStrictEqual(setResult1.failed.length, 0);
            assert.deepStrictEqual(setResult1.succeeded, [data[0], data[1], data[2]]);
            assert.deepStrictEqual(setResult2.failed.length, 0);
            assert.deepStrictEqual(setResult2.succeeded, data);
            assert.deepStrictEqual(rangeResult.error, undefined);
            const readData = data.map(e => { delete e.bytes; return e });
            assert.deepStrictEqual(rangeResult.data, [readData[0], readData[1], readData[4], readData[3]]);
        });

        it('query range should be inclusive in range.', async () => {
            //Setup
            const testShard = run.createPSSF();
            const target = new NDimensionalPartitionedSortedSet([10n, 10n, 10n], (details) => Promise.resolve(testShard));
            const data = new Array<IDimentionalData>();
            data.push({ dimensions: [1n, 0n, 0n], payload: "A", bytes: 1n });
            data.push({ dimensions: [11n, 0n, 0n], payload: "B", bytes: 1n });
            data.push({ dimensions: [21n, 0n, 0n], payload: "C", bytes: 1n });

            //Test
            const setResult = await target.write(data);
            const rangeResult = await target.rangeRead([11n, 0n, 0n], [21n, 0n, 0n]);

            //Verify
            assert.deepStrictEqual(setResult.failed.length, 0);
            assert.deepStrictEqual(setResult.succeeded, data);
            assert.deepStrictEqual(rangeResult.error, undefined);
            const readData = data.map(e => { delete e.bytes; return e });
            assert.deepStrictEqual(rangeResult.data, [readData[1], readData[2]]);
        });

    });

    describe(`"${run.type}" Purge component tests`, () => {

        beforeEach(async function () {
            client = new RedisClient(process.env.REDISCON as string);
            await client.run(["FLUSHALL"]);
        });

        afterEach(async function () {
            await client.shutdown();
        });

        it('should not purge data when nothing is matching', async () => {
            //Setup
            const testShard = run.createPSSF();
            const target = new NDimensionalPartitionedSortedSet([10n, 10n, 10n], (details) => Promise.resolve(testShard));
            const data = new Array<IDimentionalData>();
            data.push({ dimensions: [1n, 0n, 0n], payload: "A", bytes: 1n });
            data.push({ dimensions: [11n, 0n, 0n], payload: "B", bytes: 1n });
            data.push({ dimensions: [21n, 0n, 0n], payload: "C", bytes: 1n });

            //Test
            const setResult = await target.write(data);
            const purgeResult = await testShard.purgeBegin(null, null, null);
            const parsedData = target.parseTokenizedDimentionalData(purgeResult.data);

            //Verify
            assert.deepStrictEqual(setResult.failed.length, 0);
            assert.deepStrictEqual(setResult.succeeded, data);
            assert.deepStrictEqual(purgeResult.error, undefined);
            assert.deepStrictEqual(purgeResult.data.size, 0);
            assert.deepStrictEqual(parsedData.size, 0);

        });

        it('should purge data when bytes have exceeded or equal', async () => {
            //Setup
            const testShard = run.createPSSF();
            const target = new NDimensionalPartitionedSortedSet([10n, 10n, 10n], (details) => Promise.resolve(testShard));
            const data = new Array<IDimentionalData>();
            data.push({ dimensions: [1n, 0n, 0n], payload: "A", bytes: 100n });
            data.push({ dimensions: [11n, 0n, 0n], payload: "B", bytes: 13n });
            data.push({ dimensions: [21n, 0n, 0n], payload: "C", bytes: 30n });
            data.push({ dimensions: [31n, 0n, 0n], payload: "D", bytes: 10n });

            //Test
            const setResult = await target.write(data);
            const purgeResult = await testShard.purgeBegin(null, null, 10n);//Since this is partitioned each entry is pushed into different partition
            const parsedData = target.parseTokenizedDimentionalData(purgeResult.data);

            //Verify
            assert.deepStrictEqual(setResult.failed.length, 0);
            assert.deepStrictEqual(setResult.succeeded, data);
            assert.deepStrictEqual(purgeResult.error, undefined);
            const tokens = Array.from(purgeResult.data.keys());
            const parsedTokens = Array.from(parsedData.keys());
            assert.deepStrictEqual(tokens.length, 4);//4 since they are partitioned
            tokens.forEach((token, index) => {
                assert.deepStrictEqual(true, token.endsWith(purgeName));
                // @ts-ignore Below line error is valid and we want it to throw an error.
                const pData = parsedData.get(token)[0];
                assert.notDeepStrictEqual(pData, null);
                assert.notDeepStrictEqual(pData, undefined);
                // @ts-ignore Below line error is baggage of types in JS hate it :(
                const originalData = data.find(e => e.dimensions.reduce((acc, d, idx) => pData.dimensions[idx] === d && acc, true));
                assert.notDeepStrictEqual(originalData, null);
                assert.notDeepStrictEqual(originalData, undefined);
                // @ts-ignore Below line error is on typescript as it is able to detect the asserts above
                delete originalData.bytes;
                assert.deepStrictEqual(pData, originalData);
            });
            assert.deepStrictEqual(tokens, parsedTokens);
        });

        // it('should not purge data when bytes have not exceeded', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const purgeResult = await target.purgeBegin(null, null, BigInt(Number.MAX_SAFE_INTEGER));

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);
        //     assert.deepStrictEqual(purgeResult.error, undefined);
        //     assert.deepStrictEqual(purgeResult.data.size, 0);
        // });

        // it('should purge data when count has exceeded or equal', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 1n, payload: "A", setName: "small", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const purgeResult = await target.purgeBegin(null, 3, null);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);
        //     assert.deepStrictEqual(purgeResult.error, undefined);
        //     assert.deepStrictEqual(Array.from(purgeResult.data.keys()).length, 1);
        //     const token = Array.from(purgeResult.data.keys())[0];
        //     assert.deepStrictEqual(token, "Laukik" + purgeName);
        //     assert.deepStrictEqual(purgeResult.data.get(token), [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));
        // });

        // it('should not purge data when count has not exceeded', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const purgeResult = await target.purgeBegin(null, 10, null);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);
        //     assert.deepStrictEqual(purgeResult.error, undefined);
        //     assert.deepStrictEqual(purgeResult.data.size, 0);
        // });

        // it('should purge data when purge time has exceeded', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     await new Promise((acc, rej) => setTimeout(acc, 1500));//Kill time
        //     const purgeResult = await target.purgeBegin(1, null, null);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);
        //     assert.deepStrictEqual(purgeResult.error, undefined);
        //     assert.deepStrictEqual(Array.from(purgeResult.data.keys()).length, 1);
        //     const token = Array.from(purgeResult.data.keys())[0];
        //     assert.deepStrictEqual(token, "Laukik" + purgeName);
        //     assert.deepStrictEqual(purgeResult.data.get(token), [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));
        // }).timeout(-1);

        // it('should not purge data when timeout has not exceeded', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const purgeResult = await target.purgeBegin(10, null, null);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);
        //     assert.deepStrictEqual(purgeResult.error, undefined);
        //     assert.deepStrictEqual(purgeResult.data.size, 0);
        // });

        // it('should not purge data when no condition is met.', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const purgeResult = await target.purgeBegin(10, 10, 1000n);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);
        //     assert.deepStrictEqual(purgeResult.error, undefined);
        //     assert.deepStrictEqual(purgeResult.data.size, 0);
        // });

        // it('should purge data when from unfinished list first', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        //     const freshData = new Array<ISortedStringData>();
        //     freshData.push({ score: 1n, payload: "D", setName: "Fresh", bytes: 1n });
        //     freshData.push({ score: 2n, payload: "E", setName: "Fresh", bytes: 1n });
        //     freshData.push({ score: 3n, payload: "F", setName: "Fresh", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const timedoutPurgeResult = await target.purgeBegin(null, 1, null);
        //     await new Promise((acc, rej) => setTimeout(acc, 1500));//Kill time
        //     const freshSetResult = await target.upsert(freshData);
        //     const purgeResult = await target.purgeBegin(null, 100, null, 1);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);

        //     assert.deepStrictEqual(timedoutPurgeResult.error, undefined);
        //     assert.deepStrictEqual(Array.from(timedoutPurgeResult.data.keys()).length, 1);
        //     const token1 = Array.from(timedoutPurgeResult.data.keys())[0];
        //     assert.deepStrictEqual(token1, "Laukik" + purgeName);
        //     assert.deepStrictEqual(timedoutPurgeResult.data.get(token1), [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));

        //     assert.deepStrictEqual(freshSetResult.failed.length, 0);
        //     assert.deepStrictEqual(freshSetResult.succeeded, freshData);

        //     assert.deepStrictEqual(purgeResult.error, undefined);
        //     assert.deepStrictEqual(Array.from(purgeResult.data.keys()).length, 1);
        //     const token2 = Array.from(purgeResult.data.keys())[0];
        //     assert.deepStrictEqual(token2, "Laukik" + purgeName + purgeName);
        //     assert.deepStrictEqual(purgeResult.data.get(token2), [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));

        // }).timeout(-1);

        // it('should not return purged finished data', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        //     const freshData = new Array<ISortedStringData>();
        //     freshData.push({ score: 1n, payload: "D", setName: "Fresh", bytes: 1n });
        //     freshData.push({ score: 2n, payload: "E", setName: "Fresh", bytes: 1n });
        //     freshData.push({ score: 3n, payload: "F", setName: "Fresh", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const timedoutPurgeResult = await target.purgeBegin(null, 1, null);
        //     await new Promise((acc, rej) => setTimeout(acc, 1500));//Kill time
        //     const purgeFinishedResult = await target.purgeEnd(["Laukik" + purgeName]);
        //     const freshSetResult = await target.upsert(freshData);
        //     const purgeResult = await target.purgeBegin(null, 100, null, 1);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);

        //     assert.deepStrictEqual(timedoutPurgeResult.error, undefined);
        //     assert.deepStrictEqual(Array.from(timedoutPurgeResult.data.keys()).length, 1);
        //     const token1 = Array.from(timedoutPurgeResult.data.keys())[0];
        //     assert.deepStrictEqual(token1, "Laukik" + purgeName);
        //     assert.deepStrictEqual(timedoutPurgeResult.data.get(token1), [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));

        //     assert.deepStrictEqual(purgeFinishedResult.succeeded, [token1]);
        //     assert.deepStrictEqual(purgeFinishedResult.failed.length, 0);

        //     assert.deepStrictEqual(freshSetResult.failed.length, 0);
        //     assert.deepStrictEqual(freshSetResult.succeeded, freshData);

        //     assert.deepStrictEqual(purgeResult.error, undefined);
        //     assert.deepStrictEqual(purgeResult.data.size, 0);

        // }).timeout(-1);

        // it('purge finish should return false when invalid sorted upsert is mentioned.', async () => {
        //     //Setup
        //     const target = run.testTarget();

        //     //Test
        //     const purgeFinishResult = await target.purgeEnd(["ABC"]);

        //     //Verify
        //     assert.deepStrictEqual(purgeFinishResult.succeeded.length, 0);
        //     assert.deepStrictEqual(purgeFinishResult.failed.length, 1);
        //     assert.deepStrictEqual(purgeFinishResult.failed[0].error?.message, 'Token "ABC" doesnot exists.');
        // });

        // it('should read data when its purge has begun but not completed', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 1n, payload: "A", setName: "small", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const purgeResult = await target.purgeBegin(null, 3, null);
        //     const readData = await target.scoreRangeQuery("Laukik", 1n, 100n);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);
        //     assert.deepStrictEqual(purgeResult.error, undefined);
        //     assert.deepStrictEqual(Array.from(purgeResult.data.keys()).length, 1);
        //     const token1 = Array.from(purgeResult.data.keys())[0];
        //     assert.deepStrictEqual(token1, "Laukik" + purgeName);
        //     assert.deepStrictEqual(purgeResult.data.get(token1), [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));
        //     assert.deepStrictEqual(readData.error, undefined);
        //     assert.deepStrictEqual(readData.data, [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));
        // });

        // it('should not be able to read data when its purge has completed', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 1n, payload: "A", setName: "small", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const purgeResult = await target.purgeBegin(null, 3, null);
        //     const purgeCompleted = await target.purgeEnd(["Laukik" + purgeName]);
        //     const readData = await target.scoreRangeQuery("Laukik", 1n, 100n);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);
        //     assert.deepStrictEqual(purgeResult.error, undefined);
        //     assert.deepStrictEqual(Array.from(purgeResult.data.keys()).length, 1);
        //     const token1 = Array.from(purgeResult.data.keys())[0];
        //     assert.deepStrictEqual(token1, "Laukik" + purgeName);
        //     assert.deepStrictEqual(purgeResult.data.get(token1), [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));
        //     assert.deepStrictEqual(purgeCompleted.failed.length, 0);
        //     assert.deepStrictEqual(purgeCompleted.succeeded, [token1]);
        //     assert.deepStrictEqual(readData.error, undefined);
        //     assert.deepStrictEqual(readData.data, []);
        // });

        // it('should read correct data when data moves through multiple pending cycles', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        //     const freshData = new Array<ISortedStringData>();
        //     freshData.push({ score: 1n, payload: "D", setName: "Fresh", bytes: 1n });
        //     freshData.push({ score: 2n, payload: "E", setName: "Fresh", bytes: 1n });
        //     freshData.push({ score: 3n, payload: "F", setName: "Fresh", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const firstPurgeResult = await target.purgeBegin(null, 1, null);
        //     await new Promise((acc, rej) => setTimeout(acc, 1500));//Kill time
        //     const freshSetResult = await target.upsert(freshData);
        //     const secondPurgeResult = await target.purgeBegin(null, 100, null, 1);
        //     const readResultFirstSet = await target.scoreRangeQuery("Laukik", 0n, 100n);
        //     const readResultSecondSet = await target.scoreRangeQuery("Fresh", 0n, 100n);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);

        //     assert.deepStrictEqual(firstPurgeResult.error, undefined);
        //     assert.deepStrictEqual(Array.from(firstPurgeResult.data.keys()).length, 1);
        //     const token1 = Array.from(firstPurgeResult.data.keys())[0];
        //     assert.deepStrictEqual(token1, "Laukik" + purgeName);
        //     assert.deepStrictEqual(firstPurgeResult.data.get(token1), [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));

        //     assert.deepStrictEqual(freshSetResult.failed.length, 0);
        //     assert.deepStrictEqual(freshSetResult.succeeded, freshData);

        //     assert.deepStrictEqual(secondPurgeResult.error, undefined);
        //     assert.deepStrictEqual(Array.from(secondPurgeResult.data.keys()).length, 1);
        //     const token2 = Array.from(secondPurgeResult.data.keys())[0];
        //     assert.deepStrictEqual(token2, "Laukik" + purgeName + purgeName);
        //     assert.deepStrictEqual(secondPurgeResult.data.get(token2), [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));

        //     assert.deepStrictEqual(readResultFirstSet.error, undefined);
        //     assert.deepStrictEqual(readResultSecondSet.error, undefined);
        //     assert.deepStrictEqual(readResultFirstSet.data, [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));
        //     assert.deepStrictEqual(readResultSecondSet.data, [freshData[0], freshData[1], freshData[2]].map(e => { delete e.bytes; return e; }));

        // }).timeout(-1);

        // it('should read updated data when its purge has begun but not completed', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 100n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 30n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 10n });
        //     data.push({ score: 1n, payload: "A", setName: "small", bytes: 1n });
        //     const updateData = new Array<ISortedStringData>();
        //     updateData.push({ score: 53n, payload: "A", setName: "Laukik", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const purgeResult = await target.purgeBegin(null, 3, null);
        //     const updateResult = await target.upsert(updateData);
        //     const readData = await target.scoreRangeQuery("Laukik", 1n, 100n);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);
        //     assert.deepStrictEqual(purgeResult.error, undefined);
        //     assert.deepStrictEqual(Array.from(purgeResult.data.keys()).length, 1);
        //     const token1 = Array.from(purgeResult.data.keys())[0];
        //     assert.deepStrictEqual(token1, "Laukik" + purgeName);
        //     assert.deepStrictEqual(purgeResult.data.get(token1), [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));
        //     assert.deepStrictEqual(updateResult.failed.length, 0);
        //     assert.deepStrictEqual(updateResult.succeeded, updateData);
        //     assert.deepStrictEqual(readData.error, undefined);
        //     assert.deepStrictEqual(readData.data, [data[1], data[2], updateData[0]].map(e => { delete e.bytes; return e; }));
        // });

        // it('multiple purge with update should read correct data', async () => {
        //     //Setup
        //     const target = run.testTarget();
        //     const data = new Array<ISortedStringData>();
        //     data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
        //     data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });
        //     const updateData = new Array<ISortedStringData>();
        //     updateData.push({ score: 53n, payload: "A", setName: "Laukik", bytes: 1n });

        //     //Test
        //     const setResult = await target.upsert(data);
        //     const purgeResult1 = await target.purgeBegin(null, 3, null);
        //     const updateResult = await target.upsert(updateData);
        //     const purgeCompletedResult1 = await target.purgeEnd(["Laukik" + purgeName]);
        //     const readData1 = await target.scoreRangeQuery("Laukik", 1n, 100n);
        //     const purgeResult2 = await target.purgeBegin(null, 1, null);
        //     const purgeCompletedResult2 = await target.purgeEnd(["Laukik" + purgeName]);
        //     const readData2 = await target.scoreRangeQuery("Laukik", 1n, 100n);

        //     //Verify
        //     assert.deepStrictEqual(setResult.failed.length, 0);
        //     assert.deepStrictEqual(setResult.succeeded, data);
        //     assert.deepStrictEqual(purgeResult1.error, undefined);
        //     assert.deepStrictEqual(Array.from(purgeResult1.data.keys()).length, 1);
        //     const token1 = Array.from(purgeResult1.data.keys())[0];
        //     assert.deepStrictEqual(token1, "Laukik" + purgeName);
        //     assert.deepStrictEqual(purgeResult1.data.get(token1), [data[0], data[1], data[2]].map(e => { delete e.bytes; return e; }));
        //     assert.deepStrictEqual(updateResult.failed.length, 0);
        //     assert.deepStrictEqual(updateResult.succeeded, updateData);
        //     assert.deepStrictEqual(purgeCompletedResult1.failed.length, 0);
        //     assert.deepStrictEqual(purgeCompletedResult1.succeeded, [token1]);
        //     assert.deepStrictEqual(readData1.error, undefined);
        //     assert.deepStrictEqual(readData1.data, [updateData[0]].map(e => { delete e.bytes; return e; }));

        //     assert.deepStrictEqual(purgeResult2.error, undefined);
        //     assert.deepStrictEqual(Array.from(purgeResult2.data.keys()).length, 1);
        //     const token2 = Array.from(purgeResult2.data.keys())[0];
        //     assert.deepStrictEqual(token2, token1);
        //     assert.deepStrictEqual(purgeResult2.data.get(token2), [updateData[0]].map(e => { delete e.bytes; return e; }));
        //     assert.deepStrictEqual(purgeCompletedResult2.failed.length, 0);
        //     assert.deepStrictEqual(purgeCompletedResult2.succeeded, [token2]);
        //     assert.deepStrictEqual(readData2.error, undefined);
        //     assert.deepStrictEqual(readData2.data, []);
        // });

    });
});

function createRemotePsff(purkeyKey = purgeName): IPurgeableSortedSetFamily<ISortedStringData> {
    return new RemotePSSF((ops) => Promise.resolve(client), purkeyKey);
}