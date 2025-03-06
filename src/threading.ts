import { IFtpDeployArgumentsWithDefaults, Record } from "./types";
import { Worker, parentPort, workerData } from "worker_threads";
export type ActiveWorker = {
    worker: Worker;
    active: boolean;
}
export class Threading {
    constructor(numWorkers: number, args: IFtpDeployArgumentsWithDefaults) {
        this.numWorkers = numWorkers;
        this.args = args;
        this.taskQueue = [];
        this.workers = [];
    }
    private numWorkers: number;
    private args: IFtpDeployArgumentsWithDefaults;
    public taskQueue: Record[];
    private workers: ActiveWorker[];
    private async createWorkers() {
        for (let i = 0; i < this.numWorkers; i++) {
            const worker = new Worker('./src/worker.js', { workerData: { path: './worker.ts', args: this.args } });
            this.workers.push({ worker, active: false });
            worker.on('message', (msg) => {
                if (msg.type === 'taskCompleted') {
                    console.log(`Tasks remaining: ${this.taskQueue.length}`);
                    if (this.taskQueue.length > 0) {
                        const nextTask = this.taskQueue.pop();
                        worker.postMessage({ type: 'newTask', task: nextTask });
                        this.workers[i].active = true;
                    } else {
                        this.workers[i].active = false;
                    }
                }
            });
            worker.on('error', (error) => {
                console.log(`Worker error: ${error}`);
                this.workers[i].active = false;
            });
            worker.on('exit', (code) => {
                if (code !== 0) {
                    console.log(`Worker stopped with exit code ${code}`);
                }
            });
        }
    }
    public async assignInitialTasks() {
        const tasksToAssign = Math.min(this.numWorkers, this.taskQueue.length);
        for (let i = 0; i < tasksToAssign; i++) {
            const task = this.taskQueue.pop();
            if (task) {
                this.workers[i].active = true;
                this.workers[i].worker.postMessage({ type: 'newTask', task });
            }
        }
    }
    public async waitForCompletion(): Promise<void> {
        while (this.workers.some(worker => worker.active) || this.taskQueue.length > 0) {
            await new Promise(resolve => setTimeout(resolve, 100));
            console.log(`Tasks remaining: ${this.taskQueue.length}`);
            console.log(`Workers active: ${this.workers.filter(worker => worker.active).length}`);
        }
    }
    public async addTasks(tasks: Record[]) {
        this.taskQueue.push(...tasks);
        this.process();
    }
    public async process() {
        for (let i = 0; i < this.numWorkers; i++) {
            if (!this.workers[i].active && this.taskQueue.length > 0) {
                const task = this.taskQueue.pop();
                this.workers[i].active = true;
                this.workers[i].worker.postMessage({ type: 'newTask', task });
            }
        }
    }
    public async start(): Promise<void> {
        await this.createWorkers();
        await this.assignInitialTasks();
    }
    public async stop() {
        this.workers.forEach(worker => {
            worker.worker.postMessage({ type: 'exit' });
            worker.worker.terminate();
        });
        this.workers = [];
    }
}

