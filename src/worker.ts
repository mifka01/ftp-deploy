import { workerData, parentPort } from "worker_threads";
import * as ftp from "basic-ftp";
import { IFtpDeployArgumentsWithDefaults, Record } from "./types";
import { FTPSyncProvider } from "./syncProvider";
import { connect } from "./deploy";
import { ILogger } from "./utilities";

class MockedLogger implements ILogger {
    all() { }
    standard() { }
    verbose() { }
}

(async () => {
    const args: IFtpDeployArgumentsWithDefaults = workerData.args;
    const client: ftp.Client = new ftp.Client(args.timeout);
    const logger: ILogger = new MockedLogger();
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

    async function processTask(task: Record) {

        try {
            await syncProvider.syncRecordToServer(task, "upload");
            parentPort?.postMessage({ type: "taskCompleted", result: task.name });
            return true;
        } catch (error) {
            console.error("Failed to upload task", task, error);
            return false;
        }
    }

    parentPort?.on("message", async (msg) => {
        if (msg.type === "newTask") {
            const task = msg.task as Record;
            await processTask(task);
        }
    });

    parentPort?.on("exit", async () => {
        client.close();
    });

})();
