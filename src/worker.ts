import { workerData, parentPort } from "worker_threads";
import * as ftp from "basic-ftp";
import { ActionRecord, IFtpDeployArgumentsWithDefaults } from "./types";
import { FTPSyncProvider } from "./syncProvider";
import { connect } from "./deploy";
import { Logger } from "./utilities";

(async () => {
    const args: IFtpDeployArgumentsWithDefaults = workerData.args;
    let client: ftp.Client = new ftp.Client(args.timeout);
    const logger: Logger = new Logger(args['log-level']);
    const reconnectInterval = args["reconnect-timeout"] * 1000;

    let lastConnectionTime = 0;


    const timings = {
        start: () => { },
        stop: () => { },
        getTime: () => 0,
        getTimeFormatted: () => "0ms"
    }

    let syncProvider: FTPSyncProvider;

    const establishConnection = async () => {
        if (client && !client.closed) {
            client.close();
        }

        client = new ftp.Client(args.timeout);

        try {
            await connect(client, args, logger);
            lastConnectionTime = Date.now();

            syncProvider = new FTPSyncProvider(
                client,
                logger,
                timings,
                args["local-dir"],
                args["server-dir"],
                args["state-name"],
                args["dry-run"]
            );

            return true;
        } catch (error) {
            return false;
        }
    }

    if (!await establishConnection()) {
        process.exit(1);
    }

    const ensureFreshConnection = async (): Promise<boolean> => {
        const currentTime = Date.now();
        if (currentTime - lastConnectionTime >= reconnectInterval) {
            return await establishConnection();
        }
        return true;
    }

    async function processTask(task: ActionRecord): Promise<boolean> {
        try {
            if (!await ensureFreshConnection()) {
                return false;
            }

            await syncProvider.syncRecordToServer(task.record, task.action);
            parentPort?.postMessage({ type: "taskCompleted", result: task.record.name });
            return true;
        } catch (error) {
            if (error instanceof Error && error.message.includes("Not connected")) {
                if (await establishConnection()) {
                    try {
                        await syncProvider.syncRecordToServer(task.record, task.action);
                        parentPort?.postMessage({ type: "taskCompleted", result: task.record.name });
                        return true;
                    } catch (retryError) {
                        parentPort?.postMessage({ type: "taskFailed", result: { task: task, error: retryError } });
                        return false;
                    }
                }
            }
            parentPort?.postMessage({ type: "taskFailed", result: { task: task, error } });
            return false;
        }
    }

    parentPort?.on("message", async (msg) => {
        if (msg.type === "newTask") {
            const task = msg.task as ActionRecord;
            await processTask(task);
            return;
        }

        if (msg === 'exit') {
            client.close();
            console.log('Worker closed FTP connection');
            if (client.closed) {
                process.exit(0);
            }
        }
    });

})();
