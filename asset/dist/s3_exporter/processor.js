"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const job_components_1 = require("@terascope/job-components");
const utils_1 = require("@terascope/utils");
const parser_1 = require("../__lib/parser");
const slice_1 = require("../__lib/slice");
const fileName_1 = require("../__lib/fileName");
class S3Batcher extends job_components_1.BatchProcessor {
    constructor(context, opConfig, exConfig) {
        super(context, opConfig, exConfig);
        this.sliceCount = -1;
        this.client = job_components_1.getClient(context, opConfig, 's3');
        this.workerId = context.cluster.worker.id;
        this.csvOptions = parser_1.makeCsvOptions(opConfig);
        const extension = utils_1.isEmpty(opConfig.extension) ? undefined : opConfig.extension;
        this.nameOptions = {
            filePath: opConfig.path,
            extension,
            filePerSlice: opConfig.file_per_slice
        };
        // This will be incremented as the worker processes slices and used as a way to create
        // unique object names. Set to -1 so it can be incremented before any slice processing is
        // done
        this.sliceCount = -1;
        // Allows this to use the externalized name builder
    }
    async searchS3(filename, list) {
        const objPath = fileName_1.parsePath(filename);
        const objName = fileName_1.getName(this.workerId, this.sliceCount, this.nameOptions, objPath.prefix);
        const outStr = await parser_1.parseForFile(list, this.opConfig, this.csvOptions);
        // This will prevent empty objects from being added to the S3 store, which can cause
        // problems with the S3 reader
        if (!outStr || outStr.length === 0) {
            return [];
        }
        const params = {
            Bucket: objPath.bucket,
            Key: objName,
            Body: outStr
        };
        return this.client.putObject_Async(params);
    }
    async onBatch(slice) {
        // TODO also need to chunk the batches for multipart uploads
        const batches = slice_1.batchSlice(slice, this.opConfig.path);
        // Needs to be incremented before slice processing so it increments consistently for a given
        // directory
        this.sliceCount += 1;
        const actions = [];
        for (const [filename, list] of Object.entries(batches)) {
            actions.push(this.searchS3(filename, list));
        }
        await Promise.all(actions);
        return slice;
    }
}
exports.default = S3Batcher;
//# sourceMappingURL=processor.js.map