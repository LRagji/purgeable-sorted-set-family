# purgeable-sorted-set-family (PSSF)
A data structure with capabilities of "sorted-set", with additional functionality of data purge via threshold limits for memory, time, length on the set.

Few definations before we jump in:
1. `set` : A distinct collection of records, strings, number etc.
2. `sorted-set` or `ordered-set`: A set which is sorted according to a score provided with each element of the set. (set is still distinct on element content, not on score.)
3. `family`: A collection of sets.

This library provides 2 implmentation of the data structure, both expose common [interface](source/i-purgeable-sorted-set.ts) documented below.
1. [Local-PSSF](source/local-pssf.ts) Used when the applications is not horizontally scalled acorss machines and its ok to have state in the process.

2. [Remote-PSSF](source/local-pssf.ts) Used with scable applications, where application state need to be maintained on central stores. 

## Built with

1. Authors :heart for Open Source.
2. [redis-sorted-set](https://www.npmjs.com/package/redis-sorted-set) for handling local skip list implementation of sorted set.
3. [sorted-array-functions](https://www.npmjs.com/package/sorted-array-functions) for handling ordered array operations on local implementation.

## Contributions

1. New ideas/techniques are welcomed.
2. Raise a Pull Request.

## Current Version:
W.I.P(Not released yet)

## License
This project is contrubution to public domain and completely free for use, view [LICENSE.md](/license.md) file for details.

## Pre-requisite
1. When using horizontal scalling it is very important to synchronise time between shards via NTP.
2. [FOR BUILD ONLY] For windows only needs node gyp with [Visual Studio Build Tools](https://github.com/nodejs/node-gyp) with [Python](https://www.python.org/downloads/)
3. [FOR TEST/BUILD ONLY] Docker with local redis image.

## Local Build
1. `npm install`
2. `npm test`
3. Look into "./dist" folder for transpiled lib.