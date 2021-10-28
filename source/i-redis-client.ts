export interface IRedisClient {
    acquire(): Promise<void>
    release(): Promise<void>
    run(...commandArgs: string[]): Promise<any>
    pipeline(commands: string[][]): Promise<any>;
    script(filename: string, keys: string[], args: string[]): Promise<any>
}