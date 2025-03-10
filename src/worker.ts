import { workerData, parentPort } from "worker_threads";
import * as ftp from "basic-ftp";
import { ActionRecord, IFtpDeployArgumentsWithDefaults } from "./types";
import { FTPSyncProvider } from "./syncProvider";
import { connect } from "./deploy";
import { Logger } from "./utilities";


(async () => {
    const args: IFtpDeployArgumentsWithDefaults = workerData.args;
    const client: ftp.Client = new ftp.Client(args.timeout);
    const logger: Logger = new Logger(args['log-level']);

    const timings = {
        start: () => { },
        stop: () => { },
        getTime: () => 0,
        getTimeFormatted: () => "0ms"
    }

    const syncProvider = new FTPSyncProvider(
        client,
        logger,
        timings,
        args["local-dir"],
        args["server-dir"],
        args["state-name"],
        args["dry-run"]
    );

    try {
        await connect(client, args, logger);
    } catch (error) {
        console.error("Worker failed to connect to FTP server", error);
        process.exit(1);
    }

    async function processTask(task: ActionRecord): Promise<boolean> {
        try {
            await syncProvider.syncRecordToServer(task.record, task.action);
            parentPort?.postMessage({ type: "taskCompleted", result: task.record.name });
            return true;
        } catch (error) {
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
