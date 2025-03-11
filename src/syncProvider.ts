import prettyBytes from "pretty-bytes";
import type * as ftp from "basic-ftp";
import { DiffResult, ErrorCode, IFilePath, Record, FTPAction } from "./types";
import { ILogger, pluralize, retryRequest, ITimings } from "./utilities";
import { Threading } from "./threading";

export async function ensureDir(client: ftp.Client, logger: ILogger, timings: ITimings, folder: string): Promise<void> {
    timings.start("changingDir");
    logger.verbose(`  changing dir to ${folder}`);

    await retryRequest(logger, async () => await client.ensureDir(folder));

    logger.verbose(`  dir changed`);
    timings.stop("changingDir");
}

interface ISyncProvider {
    createFolder(folderPath: string): Promise<void>;
    removeFile(filePath: string): Promise<void>;
    removeFolder(folderPath: string): Promise<void>;

    /**
     * @param file file can include folder(s)
     * Note working dir is modified and NOT reset after upload
     * For now we are going to reset it - but this will be removed for performance
     */
    uploadFile(filePath: string, type: "upload" | "replace"): Promise<void>;

    syncLocalToServer(diffs: DiffResult): Promise<void>;
}

export class FTPSyncProvider implements ISyncProvider {
    constructor(client: ftp.Client, logger: ILogger, timings: ITimings, localPath: string, serverPath: string, stateName: string, dryRun: boolean,
        reconnectCallback?: () => Promise<void>
    ) {
        this.client = client;
        this.logger = logger;
        this.timings = timings;
        this.localPath = localPath;
        this.serverPath = serverPath;
        this.stateName = stateName;
        this.dryRun = dryRun;
        this.reconnectCallback = reconnectCallback;
    }

    private client: ftp.Client;
    private logger: ILogger;
    private timings: ITimings;
    private localPath: string;
    private serverPath: string;
    private dryRun: boolean;
    private stateName: string;
    private reconnectCallback: (() => Promise<void>) | undefined;


    /**
     * Converts a file path (ex: "folder/otherfolder/file.txt") to an array of folder and a file path
     * @param fullPath 
     */
    private getFileBreadcrumbs(fullPath: string): IFilePath {
        // todo see if this regex will work for nonstandard folder names
        // todo what happens if the path is relative to the root dir? (starts with /)
        const pathSplit = fullPath.split("/");
        const file = pathSplit?.pop() ?? ""; // get last item
        const folders = pathSplit.filter(folderName => folderName != "");

        return {
            folders: folders.length === 0 ? null : folders,
            file: file === "" ? null : file
        };
    }

    /**
     * Navigates up {dirCount} number of directories from the current working dir
     */
    private async upDir(dirCount: number | null | undefined): Promise<void> {
        if (typeof dirCount !== "number") {
            return;
        }

        // navigate back to the starting folder
        for (let i = 0; i < dirCount; i++) {
            await retryRequest(this.logger, async () => await this.client.cdup());
        }
    }

    async createFolder(folderPath: string) {
        // this.logger.all(`creating folder "${folderPath + "/"}"`);

        if (this.dryRun === true) {
            return;
        }

        const path = this.getFileBreadcrumbs(folderPath + "/");

        if (path.folders === null) {
            this.logger.verbose(`  no need to change dir`);
        }
        else {
            await ensureDir(this.client, this.logger, this.timings, path.folders.join("/"));
        }

        // navigate back to the root folder
        await this.upDir(path.folders?.length);

        this.logger.verbose(`  completed`);
    }

    async removeFile(filePath: string) {
        this.logger.all(`removing "${filePath}"`);

        if (this.dryRun === false) {
            try {
                await retryRequest(this.logger, async () => await this.client.remove(filePath));
            }
            catch (e: any) {
                // this error is common when a file was deleted on the server directly
                if (e.code === ErrorCode.FileNotFoundOrNoAccess) {
                    this.logger.standard("File not found or you don't have access to the file - skipping...");
                }
                else {
                    throw e;
                }
            }
        }
        this.logger.verbose(`  file removed`);

        this.logger.verbose(`  completed`);
    }

    async removeFolder(folderPath: string) {
        const absoluteFolderPath = "/" + (this.serverPath.startsWith("./") ? this.serverPath.replace("./", "") : this.serverPath) + folderPath;
        this.logger.all(`removing folder "${absoluteFolderPath}"`);

        if (this.dryRun === false) {
            await retryRequest(this.logger, async () => await this.client.removeDir(absoluteFolderPath));
        }

        this.logger.verbose(`  completed`);
    }

    async uploadFile(filePath: string, type: "upload" | "replace" = "upload") {
        // const typePresent = type === "upload" ? "uploading" : "replacing";
        const typePast = type === "upload" ? "uploaded" : "replaced";
        // this.logger.all(`${typePresent} "${filePath}"`);

        if (this.dryRun === false) {
            await retryRequest(this.logger, async () => await this.client.uploadFrom(this.localPath + filePath, filePath));
        }

        this.logger.verbose(`  file ${typePast}`);
    }

    async syncRecordToServer(record: Record, action: FTPAction) {
        if (this.reconnectCallback) {
            await this.reconnectCallback();
        }
        const actions = {
            upload: async () => {
                if (record.type === 'folder') {
                    await this.createFolder(record.name);
                } else {
                    await this.uploadFile(record.name, "upload");
                }
            },
            delete: async () => {
                if (record.type === 'folder') {
                    await this.removeFolder(record.name);
                } else {
                    await this.removeFile(record.name);
                }
            },
            replace: async () => {
                await this.uploadFile(record.name, "replace");
            }
        };

        if (actions[action]) {
            await actions[action]();
        }
    }

    printSyncHeader(diffs: DiffResult) {
        const totalCount = diffs.delete.length + diffs.upload.length + diffs.replace.length;

        this.logger.all(`----------------------------------------------------------------`);
        this.logger.all(`Making changes to ${totalCount} ${pluralize(totalCount, "file/folder", "files/folders")} to sync server state`);
        this.logger.all(`Uploading: ${prettyBytes(diffs.sizeUpload)} -- Deleting: ${prettyBytes(diffs.sizeDelete)} -- Replacing: ${prettyBytes(diffs.sizeReplace)}`);
        this.logger.all(`----------------------------------------------------------------`);
    }

    printSyncFooter() {
        this.logger.all(`----------------------------------------------------------------`);
        this.logger.all(`🎉 Sync complete. Saving current server state to "${this.serverPath + this.stateName}"`);
    }

    async syncLocalToServerMultiThread(diffs: DiffResult, threading: Threading) {
        this.printSyncHeader(diffs);


        const uploadFiles = diffs.upload.filter(item => item.type === "file" && item.name !== this.stateName);
        const topLevelFiles = uploadFiles.filter(item => item.name.split('/').length == 1);
        const allFolders = diffs.upload.filter(item => item.type === "folder");
        const replaceFiles = diffs.replace.filter(item => item.type === 'file' && item.name !== this.stateName);
        const deleteFiles = diffs.delete.filter(item => item.type === 'file');
        const deleteFolders = diffs.delete.filter(item => item.type === "folder");

        threading.addTasks(topLevelFiles, 'upload');

        const filesByFolder = new Map<string, any[]>();
        uploadFiles.forEach(item => {
            const folderName = item.name.split('/').slice(0, -1).join('/');
            if (!filesByFolder.has(folderName)) {
                filesByFolder.set(folderName, []);
            }
            filesByFolder.get(folderName)?.push(item);
        });

        for (const folder of allFolders) {
            await this.syncRecordToServer(folder, 'upload');
            const filesInFolder = filesByFolder.get(folder.name) || [];
            if (filesInFolder.length > 0) {
                threading.addTasks(filesInFolder, 'upload');
            }
        }

        for (const folder of deleteFolders) {
            await this.syncRecordToServer(folder, 'delete');
        }

        threading.addTasks(replaceFiles, 'replace');
        threading.addTasks(deleteFiles, 'delete');

        await threading.waitForAllTasks();

        this.printSyncFooter();

        if (this.dryRun === false) {
            await retryRequest(this.logger, async () => await this.client.uploadFrom(this.localPath + this.stateName, this.stateName));
        }
    }

    async syncLocalToServer(diffs: DiffResult) {
        this.printSyncHeader(diffs);

        // create new folders
        for (const folder of diffs.upload.filter(item => item.type === "folder")) {
            await this.syncRecordToServer(folder, 'upload');
        }

        // upload new files
        for (const file of diffs.upload.filter(item => item.type === "file").filter(item => item.name !== this.stateName)) {
            await this.syncRecordToServer(file, 'upload');
        }

        // replace new files
        for (const file of diffs.replace.filter(item => item.type === "file").filter(item => item.name !== this.stateName)) {
            // note: FTP will replace old files with new files. We run replacements after uploads to limit downtime
            await this.syncRecordToServer(file, 'replace');
        }

        // delete old files
        for (const file of diffs.delete.filter(item => item.type === "file")) {
            await this.syncRecordToServer(file, 'delete');
        }

        // delete old folders
        for (const folder of diffs.delete.filter(item => item.type === "folder")) {
            await this.syncRecordToServer(folder, 'delete');
        }

        this.logger.all(`----------------------------------------------------------------`);
        this.logger.all(`🎉 Sync complete. Saving current server state to "${this.serverPath + this.stateName}"`);
        if (this.dryRun === false) {
            await retryRequest(this.logger, async () => await this.client.uploadFrom(this.localPath + this.stateName, this.stateName));
        }
    }
}
