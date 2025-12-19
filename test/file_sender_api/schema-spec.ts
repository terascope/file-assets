import 'jest-extended';
import { newTestJobConfig, WorkerTestHarness } from 'teraslice-test-harness';
import { ValidatedJobConfig } from '@terascope/job-components';
import { Format } from '@terascope/file-asset-apis';
import { FileSenderAPIConfig } from '../../asset/src/file_sender_api/interfaces.js';

describe('File Sender API Schema', () => {
    let harness: WorkerTestHarness;

    async function makeTest(apiConfig: Partial<FileSenderAPIConfig> = {}) {
        const apiName = 'file_sender_api';

        const config = Object.assign(
            { _name: apiName, format: Format.ldjson },
            apiConfig
        );

        const testJob: Partial<ValidatedJobConfig> = {
            analytics: true,
            apis: [config],
            operations: [
                {
                    _op: 'file_exporter',
                    _api_name: apiName,
                    format: Format.ldjson
                },
                { _op: 'noop' },
            ],
        };

        const job = newTestJobConfig(testJob);

        harness = new WorkerTestHarness(job);
        await harness.initialize();
    }

    afterEach(async () => {
        if (harness) await harness.shutdown();
    });

    describe('when validating the schema', () => {
        it('should throw an error if no path is specified', async () => {
            await expect(makeTest({})).toReject();
        });
    });
});
