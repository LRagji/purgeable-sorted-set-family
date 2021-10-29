import { IRedisClient } from '../source/index';
import ioredis from 'ioredis';

export class RedisClient implements IRedisClient {
    private redisClient: ioredis.Redis;

    constructor(redisConnectionString: string) {
        this.redisClient = new ioredis(redisConnectionString);
    }
    async shutdown(): Promise<void> {
        await this.redisClient.quit();
        this.redisClient.disconnect();
    }
    async acquire(token: string): Promise<void> {
        console.time(token);
    }
    async release(token: string): Promise<void> {
        console.timeEnd(token);
    }

    async run(commandArgs: string[]): Promise<any> {
        //console.log(commandArgs);
        const v = await this.redisClient.send_command(commandArgs.shift() as string, ...commandArgs);
        //console.log(v);
        return v;
    }

    async pipeline(commands: string[][]): Promise<any> {
        //console.log(commands);
        const result = await this.redisClient.multi(commands)
            .exec();
        const finalResult = result.map(r => {
            let err = r.shift();
            if (err != null) {
                throw err;
            }
            return r[0];
        });
        //console.log(finalResult);
        return finalResult;
    }

    script(filename: string, keys: string[], args: string[]): Promise<any> {
        throw new Error('Method not implemented.');
    }
}