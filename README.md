# purgeable-sorted-set-family (PSSF)
A data structure with capabilities of "sorted-set", and additional functionality to purge data via threshold limits for memory, time, length on the set.
This library provides 2 implmentation of the data structure, both expose common [interface](source/i-purgeable-sorted-set.ts) documented below.
1. [Local-PSSF](source/local-pssf.ts) Used when the applications is not horizontally scalled acorss machines and its ok to have state in the process.

2. [Remote-PSSF](source/local-pssf.ts) Used with scable applications, where application state need to be maintained on central stores. 

Few definitions before we jump in:
1. `set` : A distinct collection of records, strings, number etc.
2. `sorted-set` or `ordered-set`: A set which is sorted according to a score provided with each element of the set. (set is still unique on elements content, not on score.)
3. `family`: A collection of sets.

## Example

```javascript
import { IPurgeableSortedSetFamily, IRedisClient, ISortedStringData, LocalPSSF, RemotePSSF } from 'purgeable-sorted-set-family';

const target = new LocalPSSF(); // Forsingle machine set up
//const target = new RemotePSSF((ops) => Promise.resolve(client)) //For distributed setup using redis
const data = new Array<ISortedStringData>();
data.push({ score: 1n, payload: "A", setName: "Laukik", bytes: 1n });
data.push({ score: 2n, payload: "B", setName: "Laukik", bytes: 1n });
data.push({ score: 3n, payload: "C", setName: "Laukik", bytes: 1n });

const setResult = await target.upsert(data); //Set data.
const rangeResult = await target.scoreRangeQuery("Laukik", 1n, 3n); //Query Data
const timedoutPurgeResult = await target.purgeBegin(null, 1, null); //Purge data according to Last set activity, Element Count, Bytes of the set
const purgeFinishedResult = await target.purgeEnd(Array.from(timedoutPurgeResult.data.keys)); //Ack the purge is complete.
```
More examples with component [tests](https://github.com/LRagji/purgeable-sorted-set-family/blob/main/tests/component.ts) 

## Built with
1. Authors :heart: for Open Source.
2. [redis-sorted-set](https://www.npmjs.com/package/redis-sorted-set) for handling local skip list implementation of sorted set.
3. [sorted-array-functions](https://www.npmjs.com/package/sorted-array-functions) for handling ordered array operations on local implementation.

## Contributions
1. New ideas/techniques are welcomed.
2. Raise a Pull Request.

## Current Version:
[Beta](https://github.com/LRagji/purgeable-sorted-set-family/tags)

## License
This project is contrubution to public domain and completely free for use, view [LICENSE.md](/license.md) file for details.

## Pre-requisite
1. Remote-PSSF is not suitable for redis cluster setup as it uses dynamic keys in redis scripts, but can be used with shards for different instances.
2. [FOR TEST/BUILD ONLY] Docker with local redis image.

## Local Build
1. `npm install`
2. `npm test`
3. Look into "./dist" folder for transpiled lib.