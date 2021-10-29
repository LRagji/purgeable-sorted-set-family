import { IRedisClient } from '../source/index';

export class RedisClient implements IRedisClient {
    acquire(): Promise<void> {
        throw new Error('Method not implemented.');
    }
    release(): Promise<void> {
        throw new Error('Method not implemented.');
    }
    run(...commandArgs: string[]): Promise<any> {
        throw new Error('Method not implemented.');
    }
    pipeline(commands: string[][]): Promise<any> {
        throw new Error('Method not implemented.');
    }
    script(filename: string, keys: string[], args: string[]): Promise<any> {
        throw new Error('Method not implemented.');
    }
}