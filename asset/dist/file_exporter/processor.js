"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const job_components_1 = require("@terascope/job-components");
const fs_extra_1 = __importDefault(require("fs-extra"));
const utils_1 = require("@terascope/utils");
const fileName_1 = require("../__lib/fileName");
const slice_1 = require("../__lib/slice");
const parser_1 = require("../__lib/parser");
class FileBatcher extends job_components_1.BatchProcessor {
    constructor(context, opConfig, executionConfig) {
        super(context, opConfig, executionConfig);
        const extension = utils_1.isEmpty(opConfig.extension) ? undefined : opConfig.extension;
        this.nameOptions = {
            filePath: opConfig.path,
            extension,
            filePerSlice: opConfig.file_per_slice
        };
        this.workerId = context.cluster.worker.id;
        // Coerce `file_per_slice` for JSON format or compressed output
        if ((opConfig.format === 'json') || (opConfig.compression !== 'none')) {
            this.nameOptions.filePerSlice = true;
        }
        // Used for incrementing file name with `file_per_slice`
        this.sliceCount = -1;
        this.firstSlice = true;
        // Set the options for the parser
        this.csvOptions = parser_1.makeCsvOptions(this.opConfig);
    }
    async process(path, list) {
        const fileName = fileName_1.getName(this.workerId, this.sliceCount, this.nameOptions, path);
        const outStr = await parser_1.parseForFile(list, this.opConfig, this.csvOptions);
        // Prevents empty slices from resulting in empty files
        if (!outStr || outStr.length === 0) {
            return [];
        }
        // Doesn't return a DataEntity or anything else if successful
        try {
            return fs_extra_1.default.appendFile(fileName, outStr);
        }
        catch (err) {
            throw new utils_1.TSError(err, {
                reason: `Failure to append to file ${fileName}`
            });
        }
    }
    async onBatch(slice) {
        // TODO also need to chunk the batches for multipart uploads
        const batches = slice_1.batchSlice(slice, this.opConfig.path);
        this.sliceCount += 1;
        if (!this.opConfig.file_per_slice) {
            if (this.sliceCount > 0)
                this.csvOptions.header = false;
        }
        const actions = [];
        for (const [path, list] of Object.entries(batches)) {
            actions.push(this.process(path, list));
        }
        await Promise.all(actions);
        return slice;
    }
}
exports.default = FileBatcher;
//# sourceMappingURL=processor.js.map