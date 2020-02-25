"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const job_components_1 = require("@terascope/job-components");
const path_1 = __importDefault(require("path"));
const utils_1 = require("@terascope/utils");
const parser_1 = require("../__lib/parser");
const slice_1 = require("../__lib/slice");
const fileName_1 = require("../__lib/fileName");
class HDFSBatcher extends job_components_1.BatchProcessor {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        // Client connection cannot be cached, an endpoint needs to be re-instantiated for a
        // different namenode_host
        const { connection } = opConfig;
        const clientConfig = {
            connection_cache: false,
            connection
        };
        opConfig.connection_cache = false;
        const extension = utils_1.isEmpty(opConfig.extension) ? undefined : opConfig.extension;
        this.client = job_components_1.getClient(context, clientConfig, 'hdfs_ha').client;
        this.workerId = context.cluster.worker.id;
        this.nameOptions = { filePath: opConfig.path, extension };
        // This will be incremented as the worker processes slices and used as a way to create
        // unique object names. Set to -1 so it can be incremented before any slice processing is
        // done
        this.sliceCount = -1;
        this.csvOptions = parser_1.makeCsvOptions(opConfig);
        // The append error detection and name change system need to be reworked to be compatible
        // with the file batching. In the meantime, restarting the job will sidestep the issue with
        // new worker names.
        // this.appendErrors = {};
    }
    async ensureFile(fileName) {
        try {
            return this.client.getFileStatusAsync(fileName);
        }
        catch (_err) {
            try {
                await this.client.mkdirsAsync(path_1.default.dirname(fileName));
                await this.client.createAsync(fileName, '');
            }
            catch (err) {
                new utils_1.TSError(err, {
                    reason: 'Error while attempting to create a file',
                    context: {
                        fileName
                    }
                });
            }
        }
    }
    async searchHdfs(filename, list) {
        const fileName = fileName_1.getName(this.workerId, this.sliceCount, this.nameOptions, filename);
        const outStr = await parser_1.parseForFile(list, this.opConfig, this.csvOptions);
        // This will prevent empty objects from being added to the S3 store, which can cause
        // problems with the S3 reader
        if (!outStr || outStr.length === 0) {
            return [];
        }
        await this.ensureFile(fileName);
        try {
            return this.client.appendAsync(fileName, outStr);
        }
        catch (err) {
            throw new utils_1.TSError(err, {
                reason: 'Error sending data to file',
                context: {
                    file: fileName
                }
            });
        }
    }
    async onBatch(slice) {
        // TODO also need to chunk the batches for multipart uploads
        const batches = slice_1.batchSlice(slice, this.opConfig.path);
        // Needs to be incremented before slice processing so it increments consistently for a given
        // directory
        this.sliceCount += 1;
        const actions = [];
        for (const [filename, list] of Object.entries(batches)) {
            actions.push(this.searchHdfs(filename, list));
        }
        await Promise.all(actions);
        return slice;
    }
}
exports.default = HDFSBatcher;
//# sourceMappingURL=processor.js.map