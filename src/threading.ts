import { IFtpDeployArgumentsWithDefaults, ActionRecord, Record, FTPAction } from "./types";
import { Worker } from "worker_threads";
import { URL as NodeURL } from "url";


export class Threading {
    constructor(numWorkers: number, args: IFtpDeployArgumentsWithDefaults) {
        this.numWorkers = numWorkers;
        this.args = args;
        this.taskQueue = [];
        this.workers = [];
        this.idleWorkers = [];
        this.activeTasks = 0;
    }

    private numWorkers: number;
    private args: IFtpDeployArgumentsWithDefaults;
    public taskQueue: ActionRecord[];
    private workers: Worker[];
    private idleWorkers: Worker[];
    private activeTasks: number;

    private async createWorkers() {
        for (let i = 0; i < this.numWorkers; i++) {
            const worker = new Worker(new URL("./worker.ts", import.meta.url) as NodeURL);

            worker.on('message', (msg) => {
                if (msg.type === 'taskCompleted') {
                    this.idleWorkers.push(worker);
                    this.activeTasks--;
                    this.processNextTask();
                }
                if (msg.type === 'taskFailed') {
                    // TODO!!! this should happen only if task fails with error 500 OOPS: vsf_sysutil_bind
                    // meaning that the worker cant get open port from server to connect or something like that.
                    // task should be requeued, but the worker probably shouldn't be terminated in other error cases
                    this.activeTasks--;
                    if (msg.result.task) {
                        const task = msg.result.task as ActionRecord;
                        this.taskQueue.push(task);
                        this.activeTasks++;
                    }

                    console.log('Task failed', msg.result.error);
                    console.log(msg.result.task);
                    worker.postMessage('exit');
                    worker.terminate();
                }
            });

            worker.on('error', (error) => {
                throw error;
            });

            worker.on('exit', (code) => {
                if (code !== 0) {
                    console.log(`Worker stopped with exit code ${code}`);
                }
            });

            this.workers.push(worker)
            this.idleWorkers.push(worker)
        }
    }
    public addTasks(tasks: Record[], action: FTPAction) {
        tasks.forEach(task => {
            this.taskQueue.push({ action, record: task });
        });
        this.activeTasks += tasks.length;
        this.processNextTask();
    }

    public processNextTask() {
        if (this.taskQueue.length > 0 && this.idleWorkers.length > 0) {
            const task = this.taskQueue.shift();
            if (!task) {
                return;
            }
            const worker = this.idleWorkers.shift();
            worker?.postMessage({ type: 'newTask', task });
        }
    }

    async waitForAllTasks() {
        while (this.activeTasks > 0 || this.taskQueue.length > 0) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }

    async start() {
        await this.createWorkers();
        this.processNextTask();
    }

    async stop() {
        for (const worker of this.workers) {
            worker.postMessage('exit');
            await worker.terminate();
        };

        this.workers = [];
    }
}

