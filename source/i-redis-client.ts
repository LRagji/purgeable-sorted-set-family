export interface IRedisClient {
    acquire(token?: string): Promise<void>
    release(token?: string): Promise<void>
    shutdown(): Promise<void>
    run(commandArgs: string[]): Promise<any>
    pipeline(commands: string[][]): Promise<any>;
    script(filename: string, keys: string[], args: string[]): Promise<any>
}